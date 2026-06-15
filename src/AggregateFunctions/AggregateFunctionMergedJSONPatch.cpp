#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorToString.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnObject.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromStringWithMemoryTracking.h>
#include <Common/Arena.h>
#include <Common/FieldBinaryEncoding.h>
#include <Core/Field.h>

#include <algorithm>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


struct AggregateFunctionMergedJSONPatchData
{
    /// `JSON` / `ColumnObject` cannot insert arrays that mix scalar elements with nested `JSON`
    /// object elements, because array element type inference ends up with incompatible `String`
    /// and `JSON` types. `mergedJSONPatch` follows RFC 7396 and keeps arrays as atomic replacement
    /// values, so a heterogeneous source array can survive unchanged until aggregate finalization.
    ///
    /// To avoid `NO_COMMON_TYPE` during final insertion of the aggregate result, recursively detect
    /// arrays that contain both object and non-object elements and stringify only the object
    /// elements. This preserves RFC 7396 replacement semantics for the array as a whole while
    /// converting it to a representation that the current `JSON` type can store.
    static void normalizeMixedJSONArray(Field & value)
    {
        if (value.getType() != Field::Types::Array)
            return;

        auto & array = value.safeGet<Array>();
        bool has_object = false;
        bool has_non_object = false;

        for (auto & element : array)
        {
            normalizeMixedJSONArray(element);

            if (element.getType() == Field::Types::Object)
                has_object = true;
            else
                has_non_object = true;
        }

        if (has_object && has_non_object)
        {
            for (auto & element : array)
            {
                if (element.getType() == Field::Types::Object)
                    element = Field(convertObjectToString(element.safeGet<Object>()));
            }
        }
    }

    /// Store a hierarchical patch tree with per-node winner metadata instead of a flat list
    /// of dotted paths. This removes repeated long prefixes for wide objects such as Node.js
    /// dependency maps while preserving recursive RFC 7396 object merge semantics.
    ///
    /// LIMITATION: RFC 7396 null deletion semantics (where `{"key": null}` removes a key) are NOT supported.
    /// When a JSON object like `{"key": null}` is inserted into `ColumnObject`, the null-valued
    /// key is silently dropped. `ColumnObject` cannot distinguish between "key is absent" and
    /// "key has null value", treating them as equivalent. Therefore, this aggregate function
    /// cannot detect or handle null deletion.
    struct SortKey
    {
        Field value;
        bool is_inline_int = false;
        Int64 inline_int = 0;

        SortKey() = default;

        explicit SortKey(Field value_)
            : value(std::move(value_))
        {
            if (value.getType() == Field::Types::Int64)
            {
                inline_int = value.safeGet<Int64>();
                value = Field();
                is_inline_int = true;
            }
            else if (value.getType() == Field::Types::UInt64)
            {
                inline_int = static_cast<Int64>(value.safeGet<UInt64>());
                value = Field();
                is_inline_int = true;
            }
        }

        Field toField() const
        {
            if (is_inline_int)
                return Field(inline_int);

            return value;
        }

        bool operator<(const SortKey & other) const
        {
            if (is_inline_int && other.is_inline_int)
                return inline_int < other.inline_int;

            return toField() < other.toField();
        }

        bool operator<=(const SortKey & other) const
        {
            if (is_inline_int && other.is_inline_int)
                return inline_int <= other.inline_int;

            return toField() <= other.toField();
        }

        bool operator>(const SortKey & other) const
        {
            return other < *this;
        }
    };

    struct StringSlice
    {
        const char * data = nullptr;
        size_t size = 0;

        StringSlice() = default;

        StringSlice(const char * data_, size_t size_)
            : data(data_), size(size_)
        {
        }

        std::string_view view() const
        {
            return std::string_view(data, size);
        }
    };

    static constexpr bool merged_json_patch_debug_logging = true;

    static String debugPathToString(const std::vector<String> & path)
    {
        String result;
        for (size_t i = 0; i < path.size(); ++i)
        {
            if (i != 0)
                result += ".";
            result += path[i];
        }
        return result;
    }


    struct EncodedField
    {
        enum class Kind : UInt8
        {
            Empty = 0,
            Int64 = 1,
            UInt64 = 2,
            String = 3,
            BinaryNonObjectField = 4,
            BinaryObjectField = 5,
        };

        static const char * kindToString(Kind kind)
        {
            switch (kind)
            {
                case Kind::Empty:
                    return "Empty";
                case Kind::Int64:
                    return "Int64";
                case Kind::UInt64:
                    return "UInt64";
                case Kind::String:
                    return "String";
                case Kind::BinaryNonObjectField:
                    return "BinaryNonObjectField";
                case Kind::BinaryObjectField:
                    return "BinaryObjectField";
            }

            return "Unknown";
        }

        Kind kind = Kind::Empty;
        Int64 inline_int64 = 0;
        UInt64 inline_uint64 = 0;
        StringSlice data;

        EncodedField() = default;

        explicit EncodedField(Int64 value_)
            : kind(Kind::Int64)
            , inline_int64(value_)
        {
        }

        explicit EncodedField(UInt64 value_)
            : kind(Kind::UInt64)
            , inline_uint64(value_)
        {
        }

        EncodedField(Kind kind_, StringSlice data_)
            : kind(kind_)
            , data(data_)
        {
        }

        Field get() const
        {
            switch (kind)
            {
                case Kind::Empty:
                    return {};
                case Kind::Int64:
                    return Field(inline_int64);
                case Kind::UInt64:
                    return Field(inline_uint64);
                case Kind::String:
                    return Field(String(data.view()));
                case Kind::BinaryNonObjectField:
                case Kind::BinaryObjectField:
                {
                    ReadBufferFromString buf(data.view());
                    return decodeField(buf);
                }
            }

            UNREACHABLE();
        }
    };

    struct Node;

    struct Child
    {
        StringSlice name;
        UInt32 node_index = 0;
    };

    struct Node
    {
        /// RFC 7396 says that writing a non-object value at path `a` replaces the whole subtree at `a`.
        /// In `mergedJSONPatch` we combine that with last-write-wins ordering: a whole-subtree replacement
        /// is allowed only if it is newer than every effective winner currently stored under that subtree.
        ///
        /// Partial replacement is not allowed. If any effective descendant winner is newer, the parent write
        /// cannot replace only the older descendants; it must lose entirely.
        ///
        /// To keep per-node state smaller, we do not store a cached subtree summary. Instead, when a
        /// non-object replacement is attempted, we scan the current subtree and reject the replacement if
        /// any effective winner has a larger sort key.
        std::vector<Child> children;
        EncodedField terminal_value;
        SortKey terminal_sort_key;
        bool has_terminal_value = false;
        bool has_terminal_sort_key = false;
    };

    struct DebugPathScope
    {
        std::vector<String> * stack = nullptr;

        DebugPathScope(std::vector<String> * stack_, std::string_view name)
            : stack(stack_)
        {
            if (stack)
                stack->emplace_back(name);
        }

        ~DebugPathScope()
        {
            if (stack)
                stack->pop_back();
        }
    };

    StringSlice copyPathSegment(std::string_view data)
    {
        return copyToArena(string_arena, data);
    }

    Arena string_arena;
    Arena value_arena;
    std::deque<Node> nodes;
    mutable std::vector<String> debug_deserialize_path;

    AggregateFunctionMergedJSONPatchData()
    {
        nodes.emplace_back();
    }

    static StringSlice copyToArena(Arena & arena, std::string_view data)
    {
        if (data.empty())
            return {};

        char * dst = arena.alloc(data.size());
        memcpy(dst, data.data(), data.size());
        return StringSlice(dst, data.size());
    }

    EncodedField encodeFieldToArena(Field value)
    {
        switch (value.getType())
        {
            case Field::Types::Int64:
                return EncodedField(value.safeGet<Int64>());
            case Field::Types::UInt64:
                return EncodedField(value.safeGet<UInt64>());
            case Field::Types::String:
                return EncodedField(EncodedField::Kind::String, copyToArena(value_arena, value.safeGet<String>()));
            default:
            {
                WriteBufferFromOwnString buf;
                bool is_object = value.getType() == Field::Types::Object;
                encodeField(value, buf);
                return EncodedField(
                    is_object ? EncodedField::Kind::BinaryObjectField : EncodedField::Kind::BinaryNonObjectField,
                    copyToArena(value_arena, buf.str()));
            }
        }
    }

    EncodedField cloneEncodedField(const EncodedField & value)
    {
        switch (value.kind)
        {
            case EncodedField::Kind::Empty:
                return EncodedField();
            case EncodedField::Kind::Int64:
                return EncodedField(value.inline_int64);
            case EncodedField::Kind::UInt64:
                return EncodedField(value.inline_uint64);
            case EncodedField::Kind::String:
            case EncodedField::Kind::BinaryNonObjectField:
            case EncodedField::Kind::BinaryObjectField:
                return EncodedField(value.kind, copyToArena(value_arena, value.data.view()));
        }

        UNREACHABLE();
    }

    static bool isObjectField(const Field & value)
    {
        return value.getType() == Field::Types::Object;
    }

    static void splitPath(std::string_view path, std::vector<std::string_view> & parts)
    {
        parts.clear();

        size_t start = 0;
        while (start <= path.size())
        {
            size_t dot = path.find('.', start);
            if (dot == std::string_view::npos)
            {
                parts.emplace_back(path.substr(start));
                break;
            }

            parts.emplace_back(path.substr(start, dot - start));
            start = dot + 1;
        }
    }

    static bool childNameLess(const Child & child, std::string_view name)
    {
        return child.name.view() < name;
    }

    static void clearChildren(Node & node)
    {
        node.children.clear();
    }

    static void clearNodeSubtree(Node & node)
    {
        node.has_terminal_value = false;
        node.has_terminal_sort_key = false;
        clearChildren(node);
    }

    static bool subtreeHasNewerWinner(const Node & node, const SortKey & sort_key, const std::deque<Node> & nodes)
    {
        if (node.has_terminal_sort_key && sort_key < node.terminal_sort_key)
            return true;

        for (const auto & child : node.children)
        {
            if (subtreeHasNewerWinner(nodes[child.node_index], sort_key, nodes))
                return true;
        }

        return false;
    }

    static bool subtreeReplacementWins(const Node & node, const SortKey & sort_key, const std::deque<Node> & nodes)
    {
        return !subtreeHasNewerWinner(node, sort_key, nodes);
    }

    Node & rootNode()
    {
        return nodes.front();
    }

    const Node & rootNode() const
    {
        return nodes.front();
    }

    Node & appendNode()
    {
        nodes.emplace_back();
        return nodes.back();
    }

    Node & getOrCreateChild(Node & node, std::string_view name)
    {
        auto it = std::lower_bound(
            node.children.begin(),
            node.children.end(),
            name,
            childNameLess);
        if (it != node.children.end() && it->name.view() == name)
            return nodes[it->node_index];

        Child child;
        child.name = copyPathSegment(name);
        child.node_index = static_cast<UInt32>(nodes.size());
        appendNode();
        it = node.children.insert(it, std::move(child));
        return nodes[it->node_index];
    }

    Node & appendChild(Node & node, std::string_view name)
    {
        Child child;
        child.name = copyPathSegment(name);
        child.node_index = static_cast<UInt32>(nodes.size());
        appendNode();
        node.children.push_back(std::move(child));
        return nodes[node.children.back().node_index];
    }

    void buildFieldFromNode(const Node & node, Field & out) const
    {
        if (node.has_terminal_value)
        {
            out = node.terminal_value.get();
            return;
        }

        Object object;
        for (const auto & child : node.children)
        {
            Field child_value;
            buildFieldFromNode(nodes[child.node_index], child_value);
            if (!child_value.isNull())
                object[String(child.name.view())] = std::move(child_value);
        }

        out = Field(std::move(object));
    }

    static void mergeObjectIntoNode(Node & node, const Object & object, const SortKey & sort_key, AggregateFunctionMergedJSONPatchData & owner)
    {
        if (node.has_terminal_sort_key && node.terminal_sort_key > sort_key)
            return;

        node.has_terminal_value = false;
        node.has_terminal_sort_key = false;

        for (const auto & [key, value] : object)
            owner.insertPathValue(node, key, value, sort_key);
    }

    void insertField(Node & node, Field value, const SortKey & sort_key)
    {
        normalizeMixedJSONArray(value);

        if (!isObjectField(value))
        {
            if (!subtreeReplacementWins(node, sort_key, nodes))
                return;

            clearChildren(node);
            node.terminal_value = encodeFieldToArena(std::move(value));
            node.terminal_sort_key = sort_key;
            node.has_terminal_value = true;
            node.has_terminal_sort_key = true;
            return;
        }

        if (node.has_terminal_sort_key && node.terminal_sort_key > sort_key)
            return;

        clearNodeSubtree(node);

        const auto & object = value.safeGet<Object>();
        for (const auto & [child_key, child_value] : object)
            insertPathValue(node, child_key, child_value, sort_key);
    }

    void insertPathValue(Node & start_node, std::string_view path, Field value, const SortKey & sort_key)
    {
        std::vector<std::string_view> parts;
        splitPath(path, parts);

        Node * current = &start_node;

        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (current->has_terminal_sort_key && current->terminal_sort_key > sort_key)
                return;

            bool is_last = (i + 1 == parts.size());
            if (is_last && current->has_terminal_value)
                clearNodeSubtree(*current);

            current = &getOrCreateChild(*current, parts[i]);

            if (is_last)
                insertField(*current, std::move(value), sort_key);
        }
    }

    void insertPathValue(std::string_view path, Field value, const SortKey & sort_key)
    {
        insertPathValue(rootNode(), path, std::move(value), sort_key);
    }

    static bool subtreeDominatedByNonObjectTerminal(const Node & node, const SortKey & ancestor_sort_key)
    {
        if (node.has_terminal_value && node.has_terminal_sort_key && node.terminal_sort_key <= ancestor_sort_key)
        {
            if (node.terminal_value.kind != EncodedField::Kind::BinaryObjectField)
                return true;
        }

        return false;
    }

    static void mergeNode(Node & dst, const Node & src, const AggregateFunctionMergedJSONPatchData & src_owner, AggregateFunctionMergedJSONPatchData & dst_owner)
    {
        if (src.has_terminal_value && src.has_terminal_sort_key)
        {
            if (src.terminal_value.kind != EncodedField::Kind::BinaryObjectField)
            {
                if (!dst.has_terminal_sort_key || dst.terminal_sort_key <= src.terminal_sort_key)
                {
                    clearChildren(dst);
                    dst.terminal_value = dst_owner.cloneEncodedField(src.terminal_value);
                    dst.terminal_sort_key = src.terminal_sort_key;
                    dst.has_terminal_value = true;
                    dst.has_terminal_sort_key = true;
                }

                return;
            }

            Field src_value = src.terminal_value.get();
            if (!dst.has_terminal_sort_key || dst.terminal_sort_key <= src.terminal_sort_key)
                mergeObjectIntoNode(dst, src_value.safeGet<Object>(), src.terminal_sort_key, dst_owner);
        }

        if (dst.has_terminal_value && dst.has_terminal_sort_key)
        {
            Field dst_value = dst.terminal_value.get();
            if (!isObjectField(dst_value))
                return;

            if (src.has_terminal_sort_key && src.terminal_sort_key > dst.terminal_sort_key)
            {
                dst.has_terminal_value = false;
                dst.has_terminal_sort_key = false;
            }
        }

        for (const auto & src_child : src.children)
        {
            const Node & src_child_node = src_owner.nodes[src_child.node_index];
            if (subtreeDominatedByNonObjectTerminal(dst, src_child_node.terminal_sort_key))
                continue;

            Node & dst_child = dst_owner.getOrCreateChild(dst, src_child.name.view());
            mergeNode(dst_child, src_child_node, src_owner, dst_owner);
        }

    }

    void serializeNode(const Node & node, WriteBuffer & buf) const
    {
        if constexpr (merged_json_patch_debug_logging)
        {
            if (node.has_terminal_value && !node.children.empty())
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Invariant violation in `mergedJSONPatch`: terminal node has {} children before serialization",
                    node.children.size());
            }
        }

        writeBoolText(node.has_terminal_value, buf);
        if (node.has_terminal_value)
        {
            writeBinary(static_cast<UInt8>(node.terminal_value.kind), buf);
            switch (node.terminal_value.kind)
            {
                case EncodedField::Kind::Empty:
                    break;
                case EncodedField::Kind::Int64:
                    writeVarInt(node.terminal_value.inline_int64, buf);
                    break;
                case EncodedField::Kind::UInt64:
                    writeVarUInt(node.terminal_value.inline_uint64, buf);
                    break;
                case EncodedField::Kind::String:
                case EncodedField::Kind::BinaryNonObjectField:
                case EncodedField::Kind::BinaryObjectField:
                    writeStringBinary(node.terminal_value.data.view(), buf);
                    break;
            }
            encodeField(node.terminal_sort_key.toField(), buf);
        }

        writeVarUInt(node.children.size(), buf);
        for (const auto & child : node.children)
        {
            writeStringBinary(child.name.view(), buf);
            serializeNode(nodes[child.node_index], buf);
        }
    }

    static void deserializeNodeExpanded(Node & node, ReadBuffer & buf, AggregateFunctionMergedJSONPatchData & owner)
    {
        bool has_terminal = false;
        readBoolText(has_terminal, buf);

        if (has_terminal)
        {
            UInt8 encoded_kind = 0;
            readBinary(encoded_kind, buf);

            auto kind = static_cast<EncodedField::Kind>(encoded_kind);
            if constexpr (merged_json_patch_debug_logging)
            {
                if (kind != EncodedField::Kind::Empty
                    && kind != EncodedField::Kind::Int64
                    && kind != EncodedField::Kind::UInt64
                    && kind != EncodedField::Kind::String
                    && kind != EncodedField::Kind::BinaryNonObjectField
                    && kind != EncodedField::Kind::BinaryObjectField)
                {
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Invalid terminal kind while deserializing `mergedJSONPatch` at path '{}': byte={}",
                        debugPathToString(owner.debug_deserialize_path),
                        static_cast<UInt64>(encoded_kind));
                }
            }

            switch (kind)
            {
                case EncodedField::Kind::Empty:
                    node.terminal_value = EncodedField();
                    break;
                case EncodedField::Kind::Int64:
                {
                    Int64 value = 0;
                    readVarInt(value, buf);
                    node.terminal_value = EncodedField(value);
                    break;
                }
                case EncodedField::Kind::UInt64:
                {
                    UInt64 value = 0;
                    readVarUInt(value, buf);
                    node.terminal_value = EncodedField(value);
                    break;
                }
                case EncodedField::Kind::String:
                case EncodedField::Kind::BinaryNonObjectField:
                case EncodedField::Kind::BinaryObjectField:
                {
                    size_t value_size = 0;
                    readVarUInt(value_size, buf);

                    StringSlice stored = {};
                    if (value_size)
                    {
                        char * dst = owner.value_arena.alloc(value_size);
                        buf.readStrict(dst, value_size);
                        stored = StringSlice(dst, value_size);
                    }

                    node.terminal_value = EncodedField(kind, stored);
                    break;
                }
            }

            Field terminal_sort_key = decodeField(buf);
            node.terminal_sort_key = SortKey(std::move(terminal_sort_key));

            node.has_terminal_value = true;
            node.has_terminal_sort_key = true;
        }

        size_t children_size = 0;
        readVarUInt(children_size, buf);
        node.children.clear();
        node.children.reserve(children_size);

        String key;
        for (size_t i = 0; i < children_size; ++i)
        {
            key.clear();
            readStringBinary(key, buf);

            Node & child = owner.appendChild(node, key);
            DebugPathScope path_scope(merged_json_patch_debug_logging ? &owner.debug_deserialize_path : nullptr, key);
            deserializeNodeExpanded(child, buf, owner);
        }

    }

    void deserializeNode(Node & node, ReadBuffer & buf)
    {
        node.has_terminal_value = false;
        node.has_terminal_sort_key = false;
        node.terminal_value = EncodedField();
        node.children.clear();

        deserializeNodeExpanded(node, buf, *this);
    }

    void collectObject(const Node & node, Object & out) const
    {
        if (node.has_terminal_value)
        {
            Field value = node.terminal_value.get();
            if (isObjectField(value))
            {
                const auto & object = value.safeGet<Object>();
                for (const auto & [key, child_value] : object)
                    out[key] = child_value;
            }
            return;
        }

        for (const auto & child : node.children)
        {
            Field child_value;
            buildFieldFromNode(nodes[child.node_index], child_value);

            if (!child_value.isNull())
                out[String(child.name.view())] = std::move(child_value);
        }
    }

    void add(const IColumn & json_column, size_t row_num, Arena * arena)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);

        const char * begin = nullptr;
        auto serialized = object_column.serializeValueIntoArena(row_num, *arena, begin, nullptr);
        SortKey sort_key = SortKey(Field(String(serialized)));

        addKeyValuePairs(object_column, row_num, sort_key);
    }

    void addWithKey(const IColumn & json_column, const IColumn & key_column, size_t row_num, Arena *)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        SortKey sort_key = SortKey(key_column[row_num]);
        addKeyValuePairs(object_column, row_num, sort_key);
    }

    void addKeyValuePairs(const ColumnObject & object_column, size_t row_num, const SortKey & sort_key)
    {
        ColumnObject::SortedPathsIterator it(object_column, row_num);
        while (!it.end())
        {
            auto path_info = it.getCurrentPathInfo();

            Field value;
            path_info.column->get(path_info.row, value);
            normalizeMixedJSONArray(value);

            insertPathValue(path_info.path, std::move(value), sort_key);
            it.next();
        }
    }

    void merge(const AggregateFunctionMergedJSONPatchData & other, Arena *)
    {
        mergeNode(rootNode(), other.rootNode(), other, *this);
    }

    void serialize(WriteBuffer & buf) const
    {
        serializeNode(rootNode(), buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        nodes.clear();
        nodes.emplace_back();
        debug_deserialize_path.clear();
        deserializeNode(rootNode(), buf);
    }

    void insertResultInto(IColumn & to, const DataTypePtr &) const
    {
        auto & result_column = assert_cast<ColumnObject &>(to);

        Object result_object;
        collectObject(rootNode(), result_object);

        if (result_object.empty())
        {
            result_column.insertDefault();
            return;
        }

        result_column.insert(Field(result_object));
    }
};


class AggregateFunctionMergedJSONPatch final
    : public IAggregateFunctionDataHelper<AggregateFunctionMergedJSONPatchData, AggregateFunctionMergedJSONPatch>
{
private:
    bool has_sort_key;

public:
    explicit AggregateFunctionMergedJSONPatch(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionMergedJSONPatchData, AggregateFunctionMergedJSONPatch>(
            argument_types_, {}, argument_types_[0])
        , has_sort_key(argument_types_.size() > 1)
    {
    }

    String getName() const override
    {
        return "mergedJSONPatch";
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (has_sort_key)
            data(place).addWithKey(*columns[0], *columns[1], row_num, arena);
        else
            data(place).add(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).merge(data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        data(place).insertResultInto(to, result_type);
    }
};


AggregateFunctionPtr createAggregateFunctionMergedJSONPatch(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);

    if (argument_types.size() != 1 && argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}. Expected 1 or 2 arguments (JSON value and optional sort key), got {} arguments",
            name, argument_types.size());

    if (!isObject(argument_types[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of first argument for aggregate function {}. Expected type JSON",
            argument_types[0]->getName(), name);

    if (argument_types.size() == 2 && !argument_types[1]->isComparable())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of second argument for aggregate function {}. Expected comparable type for sort key",
            argument_types[1]->getName(), name);

    return std::make_shared<AggregateFunctionMergedJSONPatch>(argument_types);
}


void registerAggregateFunctionMergedJSONPatch(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {
        .returns_default_when_only_null = false,
        .is_order_dependent = false  // Changed to false since we sort before merging
    };

    FunctionDocumentation::Description description = R"(
Aggregates JSON values by merging them with last-write-wins semantics, implementing the core merge
behavior of RFC 7396 JSON Merge Patch at the path level.

The aggregate function stores state as triplets (key, value, sorting_key) where each key (JSON path)
only keeps the latest record according to the sorting_key. This enables distributed queries to work
correctly by merging states at the final stage.

When called with one argument `mergedJSONPatch(json_col)`, a deterministic sort key is generated
from the serialized JSON object to ensure consistent ordering across distributed queries.

When called with two arguments `mergedJSONPatch(json_col, sort_key)`, the provided sort_key
determines which value wins for each JSON path. The value with the largest sort_key is retained.

During distributed query execution, each shard maintains a map of (path → (value, sort_key)).
When merging states from multiple shards, for each path, only the value with the largest sort_key
is kept. This implements RFC 7396 semantics where later values override earlier ones for the same keys,
ensuring consistent results regardless of shard processing order.

LIMITATION: RFC 7396 null deletion semantics (where `{"key": null}` removes a key) are not supported.
ColumnObject silently drops null-valued keys during insertion, making it impossible to distinguish
between absent keys and keys with null values.
)";

    FunctionDocumentation::Syntax syntax = "mergedJSONPatch(json)";

    FunctionDocumentation::Arguments arguments = {
        {"json", "JSON column to aggregate.", {"JSON"}}
    };

    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a single JSON object that is the result of merging all input JSON objects.",
        {"JSON"}
    };

    FunctionDocumentation::Examples examples = {
        {
            "Basic usage with sort key",
            R"(
SELECT mergedJSONPatch(json, sort_key) FROM
(
    SELECT '{"a":1}'::JSON AS json, 1 AS sort_key
    UNION ALL
    SELECT '{"b":2}'::JSON, 2
    UNION ALL
    SELECT '{"a":3, "c":4}'::JSON, 3
);
            )",
            R"(
┌─mergedJSONPatch(json, sort_key)─┐
│ {"a":3,"b":2,"c":4}              │
└──────────────────────────────────┘
            )"
        }
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;

    FunctionDocumentation documentation = {
        description,
        syntax,
        arguments,
        {},
        returned_value,
        examples,
        introduced_in,
        category
    };

    factory.registerFunction("mergedJSONPatch", {createAggregateFunctionMergedJSONPatch, documentation, properties});
}

}

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

    struct SerializedChildSubtree
    {
        StringSlice name;
        UInt64 offset = 0;
        UInt64 size = 0;
    };

    struct Node
    {
        /// RFC 7396 says that writing a non-object value at path `a` replaces the whole subtree at `a`.
        /// In `mergedJSONPatch` we combine that with last-write-wins ordering: a whole-subtree replacement
        /// is allowed only if it is newer than every effective winner currently stored under that subtree.
        ///
        /// To implement this without storing per-descendant summaries, each node tracks the maximum sort key
        /// among effective winners in its current subtree. Parent replacement can then compare against a
        /// single summary value:
        /// - if `sort_key >= subtree_max_sort_key`, the whole replacement wins;
        /// - otherwise the whole replacement loses.
        ///
        /// Partial replacement is not allowed. If any effective descendant winner is newer, the parent write
        /// cannot replace only the older descendants; it must lose entirely.
        std::vector<Child> children;
        EncodedField terminal_value;
        SortKey terminal_sort_key;
        SortKey subtree_max_sort_key;
        StringSlice serialized_subtree;
        UInt64 serialized_subtree_top_level_children = 0;
        std::vector<SerializedChildSubtree> serialized_child_subtrees;
        bool has_terminal_value = false;
        bool has_terminal_sort_key = false;
        bool has_subtree_max_sort_key = false;
        bool has_serialized_subtree = false;
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

    static void clearSerializedSubtree(Node & node)
    {
        node.has_serialized_subtree = false;
        node.serialized_subtree = {};
        node.serialized_subtree_top_level_children = 0;
        node.serialized_child_subtrees.clear();
    }

    static void clearNodeSubtree(Node & node)
    {
        node.has_terminal_value = false;
        node.has_terminal_sort_key = false;
        node.has_subtree_max_sort_key = false;
        clearSerializedSubtree(node);
        clearChildren(node);
    }

    static void refreshSubtreeMaxSortKey(Node & node, const std::deque<Node> & nodes)
    {
        node.has_subtree_max_sort_key = false;

        if (node.has_terminal_sort_key)
        {
            node.subtree_max_sort_key = node.terminal_sort_key;
            node.has_subtree_max_sort_key = true;
        }

        for (const auto & child : node.children)
        {
            const Node & child_node = nodes[child.node_index];
            if (!child_node.has_subtree_max_sort_key)
                continue;

            if (!node.has_subtree_max_sort_key || node.subtree_max_sort_key < child_node.subtree_max_sort_key)
            {
                node.subtree_max_sort_key = child_node.subtree_max_sort_key;
                node.has_subtree_max_sort_key = true;
            }
        }
    }

    static bool subtreeReplacementWins(const Node & node, const SortKey & sort_key)
    {
        return !node.has_subtree_max_sort_key || node.subtree_max_sort_key <= sort_key;
    }

    static const SerializedChildSubtree * findSerializedChildSubtree(const Node & node, std::string_view name)
    {
        auto it = std::lower_bound(
            node.serialized_child_subtrees.begin(),
            node.serialized_child_subtrees.end(),
            name,
            [](const SerializedChildSubtree & child, std::string_view child_name)
            {
                return child.name.view() < child_name;
            });

        if (it == node.serialized_child_subtrees.end() || it->name.view() != name)
            return nullptr;

        return &*it;
    }

    static void copySerializedSubtreeToNode(
        Node & dst,
        const Node & src_node,
        const SerializedChildSubtree & src_child_subtree,
        AggregateFunctionMergedJSONPatchData & dst_owner)
    {
        clearNodeSubtree(dst);
        dst.has_serialized_subtree = true;
        dst.serialized_subtree_top_level_children = 0;
        dst.serialized_subtree = copyToArena(
            dst_owner.value_arena,
            std::string_view(src_node.serialized_subtree.data + src_child_subtree.offset, src_child_subtree.size));
    }

    static void materializeSerializedChildren(
        Node & dst,
        std::span<const StringSlice> names,
        AggregateFunctionMergedJSONPatchData & dst_owner)
    {
        if (!dst.has_serialized_subtree || names.empty())
            return;

        std::vector<SerializedChildSubtree> matched_child_subtrees;
        matched_child_subtrees.reserve(names.size());

        for (const auto & name : names)
        {
            const auto * child_subtree = findSerializedChildSubtree(dst, name.view());
            if (child_subtree)
                matched_child_subtrees.push_back(*child_subtree);
        }

        if (matched_child_subtrees.empty())
            return;

        std::sort(
            matched_child_subtrees.begin(),
            matched_child_subtrees.end(),
            [](const SerializedChildSubtree & lhs, const SerializedChildSubtree & rhs)
            {
                return lhs.name.view() < rhs.name.view();
            });

        StringSlice serialized_subtree = dst.serialized_subtree;

        std::vector<SerializedChildSubtree> remaining_child_subtrees;
        remaining_child_subtrees.reserve(
            dst.serialized_child_subtrees.size() > matched_child_subtrees.size()
                ? dst.serialized_child_subtrees.size() - matched_child_subtrees.size()
                : 0);

        size_t matched_index = 0;
        for (const auto & child_subtree : dst.serialized_child_subtrees)
        {
            if (matched_index < matched_child_subtrees.size()
                && child_subtree.name.view() == matched_child_subtrees[matched_index].name.view())
            {
                ++matched_index;
                continue;
            }

            remaining_child_subtrees.push_back(child_subtree);
        }

        dst.serialized_child_subtrees = std::move(remaining_child_subtrees);
        dst.serialized_subtree_top_level_children = dst.serialized_child_subtrees.size();

        for (const auto & child_subtree : matched_child_subtrees)
        {
            auto it = std::lower_bound(
                dst.children.begin(),
                dst.children.end(),
                child_subtree.name.view(),
                childNameLess);

            if (it != dst.children.end() && it->name.view() == child_subtree.name.view())
                continue;

            Child child;
            child.name = copyToArena(dst_owner.string_arena, child_subtree.name.view());
            child.node_index = static_cast<UInt32>(dst_owner.nodes.size());
            dst_owner.appendNode();
            Node & dst_child = dst_owner.nodes[child.node_index];
            dst_child.has_serialized_subtree = true;
            dst_child.serialized_subtree_top_level_children = 0;
            dst_child.serialized_subtree = copyToArena(
                dst_owner.value_arena,
                std::string_view(serialized_subtree.data + child_subtree.offset, child_subtree.size));
            dst.children.insert(it, std::move(child));
        }

        if (dst.serialized_child_subtrees.empty())
            clearSerializedSubtree(dst);
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
        if (node.has_serialized_subtree)
        {
            Node expanded = node;
            auto & mutable_self = const_cast<AggregateFunctionMergedJSONPatchData &>(*this);
            ensureExpanded(expanded, mutable_self);
            buildFieldFromNode(expanded, out);
            return;
        }

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

    static EncodedField cloneEncodedField(const EncodedField & src, AggregateFunctionMergedJSONPatchData & dst_owner)
    {
        switch (src.kind)
        {
            case EncodedField::Kind::Empty:
                return EncodedField();
            case EncodedField::Kind::Int64:
                return EncodedField(src.inline_int64);
            case EncodedField::Kind::UInt64:
                return EncodedField(src.inline_uint64);
            case EncodedField::Kind::String:
            case EncodedField::Kind::BinaryNonObjectField:
            case EncodedField::Kind::BinaryObjectField:
                return EncodedField(src.kind, copyToArena(dst_owner.value_arena, src.data.view()));
        }

        UNREACHABLE();
    }

    static UInt32 cloneSubtree(const AggregateFunctionMergedJSONPatchData & src_owner, UInt32 src_node_index, AggregateFunctionMergedJSONPatchData & dst_owner)
    {
        const Node & src_node = src_owner.nodes[src_node_index];
        Node & dst_node = dst_owner.appendNode();

        dst_node.has_terminal_value = src_node.has_terminal_value;
        dst_node.has_terminal_sort_key = src_node.has_terminal_sort_key;
        dst_node.terminal_sort_key = src_node.terminal_sort_key;
        dst_node.has_subtree_max_sort_key = src_node.has_subtree_max_sort_key;
        dst_node.subtree_max_sort_key = src_node.subtree_max_sort_key;
        dst_node.has_serialized_subtree = src_node.has_serialized_subtree;
        dst_node.serialized_subtree_top_level_children = src_node.serialized_subtree_top_level_children;
        if (src_node.has_terminal_value)
            dst_node.terminal_value = cloneEncodedField(src_node.terminal_value, dst_owner);
        if (src_node.has_serialized_subtree)
            dst_node.serialized_subtree = copyToArena(dst_owner.value_arena, src_node.serialized_subtree.view());

        dst_node.children.reserve(src_node.children.size());
        for (const auto & src_child : src_node.children)
        {
            Child dst_child;
            dst_child.name = copyToArena(dst_owner.string_arena, src_child.name.view());
            dst_child.node_index = cloneSubtree(src_owner, src_child.node_index, dst_owner);
            dst_node.children.push_back(dst_child);
        }

        return static_cast<UInt32>(dst_owner.nodes.size() - 1);
    }

    static void overwriteNodeWithSubtree(Node & dst, const AggregateFunctionMergedJSONPatchData & src_owner, UInt32 src_node_index, AggregateFunctionMergedJSONPatchData & dst_owner)
    {
        const Node & src_node = src_owner.nodes[src_node_index];

        dst.children.clear();
        dst.has_terminal_value = src_node.has_terminal_value;
        dst.has_terminal_sort_key = src_node.has_terminal_sort_key;
        dst.terminal_sort_key = src_node.terminal_sort_key;
        dst.has_subtree_max_sort_key = src_node.has_subtree_max_sort_key;
        dst.subtree_max_sort_key = src_node.subtree_max_sort_key;
        dst.terminal_value = src_node.has_terminal_value ? cloneEncodedField(src_node.terminal_value, dst_owner) : EncodedField();
        dst.has_serialized_subtree = src_node.has_serialized_subtree;
        dst.serialized_subtree_top_level_children = src_node.serialized_subtree_top_level_children;
        dst.serialized_subtree = src_node.has_serialized_subtree
            ? copyToArena(dst_owner.value_arena, src_node.serialized_subtree.view())
            : StringSlice{};
        dst.serialized_child_subtrees.clear();
        dst.serialized_child_subtrees.reserve(src_node.serialized_child_subtrees.size());
        for (const auto & src_child_subtree : src_node.serialized_child_subtrees)
        {
            SerializedChildSubtree dst_child_subtree;
            dst_child_subtree.name = copyToArena(dst_owner.string_arena, src_child_subtree.name.view());
            dst_child_subtree.offset = src_child_subtree.offset;
            dst_child_subtree.size = src_child_subtree.size;
            dst.serialized_child_subtrees.push_back(dst_child_subtree);
        }

        if (src_node.has_serialized_subtree)
            return;

        dst.children.reserve(src_node.children.size());
        for (const auto & src_child : src_node.children)
        {
            Child dst_child;
            dst_child.name = copyToArena(dst_owner.string_arena, src_child.name.view());
            dst_child.node_index = cloneSubtree(src_owner, src_child.node_index, dst_owner);
            dst.children.push_back(dst_child);
        }
    }

    static bool canOverwriteWithSubtree(const Node & dst, const Node & src)
    {
        if (!src.has_terminal_sort_key)
            return false;

        if (dst.has_terminal_sort_key && dst.terminal_sort_key > src.terminal_sort_key)
            return false;

        if (!dst.children.empty() || dst.has_serialized_subtree)
            return false;

        if (!dst.has_terminal_value)
            return true;

        return !isObjectField(dst.terminal_value.get());
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
            if (!subtreeReplacementWins(node, sort_key))
                return;

            clearChildren(node);
            clearSerializedSubtree(node);
            node.terminal_value = encodeFieldToArena(std::move(value));
            node.terminal_sort_key = sort_key;
            node.subtree_max_sort_key = sort_key;
            node.has_terminal_value = true;
            node.has_terminal_sort_key = true;
            node.has_subtree_max_sort_key = true;
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
        std::vector<Node *> visited_nodes;
        visited_nodes.reserve(parts.size() + 1);
        visited_nodes.push_back(current);

        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (current->has_terminal_sort_key && current->terminal_sort_key > sort_key)
                return;

            bool is_last = (i + 1 == parts.size());
            if (is_last && current->has_terminal_value)
                clearNodeSubtree(*current);

            current = &getOrCreateChild(*current, parts[i]);
            visited_nodes.push_back(current);

            if (is_last)
                insertField(*current, std::move(value), sort_key);
        }

        for (auto it = visited_nodes.rbegin(); it != visited_nodes.rend(); ++it)
            refreshSubtreeMaxSortKey(**it, nodes);
    }

    void insertPathValue(std::string_view path, Field value, const SortKey & sort_key)
    {
        insertPathValue(rootNode(), path, std::move(value), sort_key);
    }

    static bool copyTerminalValueIfDominates(Node & dst, const Node & src, AggregateFunctionMergedJSONPatchData & dst_owner)
    {
        if (!src.has_terminal_value || !src.has_terminal_sort_key)
            return false;

        if (src.terminal_value.kind == EncodedField::Kind::BinaryObjectField)
            return false;

        if (dst.has_terminal_sort_key && dst.terminal_sort_key > src.terminal_sort_key)
            return true;

        clearChildren(dst);

        switch (src.terminal_value.kind)
        {
            case EncodedField::Kind::Empty:
                dst.terminal_value = EncodedField();
                break;
            case EncodedField::Kind::Int64:
                dst.terminal_value = EncodedField(src.terminal_value.inline_int64);
                break;
            case EncodedField::Kind::UInt64:
                dst.terminal_value = EncodedField(src.terminal_value.inline_uint64);
                break;
            case EncodedField::Kind::String:
            case EncodedField::Kind::BinaryNonObjectField:
                dst.terminal_value = EncodedField(
                    src.terminal_value.kind,
                    copyToArena(dst_owner.value_arena, src.terminal_value.data.view()));
                break;
            case EncodedField::Kind::BinaryObjectField:
                UNREACHABLE();
        }

        dst.terminal_sort_key = src.terminal_sort_key;
        dst.subtree_max_sort_key = src.terminal_sort_key;
        dst.has_terminal_value = true;
        dst.has_terminal_sort_key = true;
        dst.has_subtree_max_sort_key = true;
        return true;
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

    static void ensureExpanded(Node & node, AggregateFunctionMergedJSONPatchData & owner)
    {
        if (!node.has_serialized_subtree)
            return;

        ReadBufferFromMemory subtree_buf(node.serialized_subtree.data, node.serialized_subtree.size);
        node.has_serialized_subtree = false;
        node.serialized_subtree = {};
        node.serialized_subtree_top_level_children = 0;
        node.serialized_child_subtrees.clear();
        node.has_terminal_value = false;
        node.has_terminal_sort_key = false;
        node.has_subtree_max_sort_key = false;
        node.terminal_value = EncodedField();
        node.children.clear();
        deserializeNodeExpanded(node, subtree_buf, owner);
    }

    static void mergeNode(Node & dst, const Node & src, const AggregateFunctionMergedJSONPatchData & src_owner, AggregateFunctionMergedJSONPatchData & dst_owner)
    {
        if (copyTerminalValueIfDominates(dst, src, dst_owner))
            return;

        if (src.has_serialized_subtree && !src.has_terminal_value && src.children.empty())
        {
            if (!dst.has_terminal_value && dst.children.empty() && !dst.has_serialized_subtree)
            {
                dst.has_terminal_value = false;
                dst.has_terminal_sort_key = false;
                dst.terminal_value = EncodedField();
                dst.has_serialized_subtree = true;
                dst.serialized_subtree_top_level_children = src.serialized_subtree_top_level_children;
                dst.serialized_subtree = copyToArena(dst_owner.value_arena, src.serialized_subtree.view());
                return;
            }

            if (canOverwriteWithSubtree(dst, src))
            {
                dst.children.clear();
                dst.has_terminal_value = false;
                dst.has_terminal_sort_key = src.has_terminal_sort_key;
                dst.terminal_sort_key = src.terminal_sort_key;
                dst.terminal_value = EncodedField();
                dst.has_serialized_subtree = true;
                dst.serialized_subtree_top_level_children = src.serialized_subtree_top_level_children;
                dst.serialized_subtree = copyToArena(dst_owner.value_arena, src.serialized_subtree.view());
                return;
            }

        }

        if (dst.has_serialized_subtree
            && dst.serialized_subtree_top_level_children == 0
            && !src.has_serialized_subtree
            && src.children.empty())
        {
            if (copyTerminalValueIfDominates(dst, src, dst_owner))
                return;
        }

        if (dst.has_serialized_subtree
            && !dst.serialized_child_subtrees.empty()
            && !src.children.empty())
        {
            std::vector<StringSlice> overlapping_names;
            overlapping_names.reserve(src.children.size());

            for (const auto & src_child : src.children)
            {
                if (findSerializedChildSubtree(dst, src_child.name.view()))
                    overlapping_names.push_back(src_child.name);
            }

            if (!overlapping_names.empty() && overlapping_names.size() < dst.serialized_subtree_top_level_children)
                materializeSerializedChildren(dst, overlapping_names, dst_owner);
        }

        ensureExpanded(dst, dst_owner);

        if (src.has_serialized_subtree && !src.has_terminal_value && src.children.empty())
        {
            AggregateFunctionMergedJSONPatchData expanded_src_owner;
            expanded_src_owner.debug_deserialize_path = src_owner.debug_deserialize_path;
            Node & expanded_src_root = expanded_src_owner.rootNode();
            expanded_src_root.has_serialized_subtree = true;
            expanded_src_root.serialized_subtree = copyToArena(expanded_src_owner.value_arena, src.serialized_subtree.view());
            ensureExpanded(expanded_src_root, expanded_src_owner);
            mergeNode(dst, expanded_src_root, expanded_src_owner, dst_owner);
            return;
        }

        if (src.has_terminal_value && src.has_terminal_sort_key)
        {
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

            auto it = std::lower_bound(
                dst.children.begin(),
                dst.children.end(),
                src_child.name.view(),
                childNameLess);

            if (it == dst.children.end() || it->name.view() != src_child.name.view())
            {
                if (canOverwriteWithSubtree(dst, src_child_node))
                {
                    Child child;
                    child.name = copyToArena(dst_owner.string_arena, src_child.name.view());
                    child.node_index = cloneSubtree(src_owner, src_child.node_index, dst_owner);
                    dst.children.insert(it, std::move(child));
                    continue;
                }

                Node & dst_child = dst_owner.getOrCreateChild(dst, src_child.name.view());
                mergeNode(dst_child, src_child_node, src_owner, dst_owner);
                continue;
            }

            Node & dst_child = dst_owner.nodes[it->node_index];
            if (canOverwriteWithSubtree(dst_child, src_child_node))
            {
                overwriteNodeWithSubtree(dst_child, src_owner, src_child.node_index, dst_owner);
                continue;
            }

            mergeNode(dst_child, src_child_node, src_owner, dst_owner);
        }

        refreshSubtreeMaxSortKey(dst, dst_owner.nodes);
    }

    void serializeNode(const Node & node, WriteBuffer & buf) const
    {
        if (node.has_serialized_subtree)
        {
            buf.write(node.serialized_subtree.data, node.serialized_subtree.size);
            return;
        }

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

    struct SerializedSubtreeBuilder
    {
        struct NodeFrame
        {
            size_t offset = 0;
            std::vector<SerializedChildSubtree> child_subtrees;
        };

        String data;
        std::vector<NodeFrame> node_frames;

        void append(std::string_view value)
        {
            data.append(value.data(), value.size());
        }

        void appendByte(UInt8 value)
        {
            data.push_back(static_cast<char>(value));
        }

        void appendBoolText(bool value)
        {
            append(value ? std::string_view("true", 4) : std::string_view("false", 5));
        }

        void appendVarUInt(UInt64 value)
        {
            while (value >= 0x80)
            {
                appendByte(static_cast<UInt8>(value) | 0x80);
                value >>= 7;
            }

            appendByte(static_cast<UInt8>(value));
        }

        void appendVarInt(Int64 value)
        {
            appendVarUInt((static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63));
        }

        void appendStringBinary(std::string_view value)
        {
            appendVarUInt(value.size());
            append(value);
        }

        void appendEncodedFieldBinary(const Field & value)
        {
            WriteBufferFromOwnString buf;
            encodeField(value, buf);
            buf.finalize();
            append(buf.str());
        }

        void beginNode()
        {
            NodeFrame frame;
            frame.offset = data.size();
            node_frames.push_back(std::move(frame));
        }

        void recordChildSubtree(std::string_view name, size_t child_offset, size_t child_size)
        {
            auto & frame = node_frames.back();
            frame.child_subtrees.push_back(SerializedChildSubtree{
                .name = StringSlice(name.data(), name.size()),
                .offset = child_offset - frame.offset,
                .size = child_size,
            });
        }

        std::pair<std::string_view, const std::vector<SerializedChildSubtree> &> finishNode() const
        {
            const auto & frame = node_frames.back();
            return {
                std::string_view(data.data() + frame.offset, data.size() - frame.offset),
                frame.child_subtrees
            };
        }

        void popNode()
        {
            node_frames.pop_back();
        }
    };

    static void deserializeNodeExpanded(Node & node, ReadBuffer & buf, AggregateFunctionMergedJSONPatchData & owner, SerializedSubtreeBuilder * capture = nullptr)
    {
        if (capture)
            capture->beginNode();

        bool has_terminal = false;
        readBoolText(has_terminal, buf);
        if (capture)
            capture->appendBoolText(has_terminal);

        if (has_terminal)
        {
            UInt8 encoded_kind = 0;
            readBinary(encoded_kind, buf);
            if (capture)
                capture->appendByte(encoded_kind);

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
                    if (capture)
                        capture->appendVarInt(value);
                    node.terminal_value = EncodedField(value);
                    break;
                }
                case EncodedField::Kind::UInt64:
                {
                    UInt64 value = 0;
                    readVarUInt(value, buf);
                    if (capture)
                        capture->appendVarUInt(value);
                    node.terminal_value = EncodedField(value);
                    break;
                }
                case EncodedField::Kind::String:
                case EncodedField::Kind::BinaryNonObjectField:
                case EncodedField::Kind::BinaryObjectField:
                {
                    size_t value_size = 0;
                    readVarUInt(value_size, buf);
                    if (capture)
                        capture->appendVarUInt(value_size);

                    StringSlice stored = {};
                    if (value_size)
                    {
                        char * dst = owner.value_arena.alloc(value_size);
                        buf.readStrict(dst, value_size);
                        stored = StringSlice(dst, value_size);

                        if (capture)
                            capture->append(std::string_view(dst, value_size));
                    }

                    node.terminal_value = EncodedField(kind, stored);
                    break;
                }
            }

            Field terminal_sort_key = decodeField(buf);
            if (capture)
                capture->appendEncodedFieldBinary(terminal_sort_key);
            node.terminal_sort_key = SortKey(std::move(terminal_sort_key));
            node.subtree_max_sort_key = node.terminal_sort_key;

            node.has_terminal_value = true;
            node.has_terminal_sort_key = true;
            node.has_subtree_max_sort_key = true;
        }

        size_t children_size = 0;
        readVarUInt(children_size, buf);
        if (capture)
            capture->appendVarUInt(children_size);
        node.children.clear();
        node.children.reserve(children_size);

        String key;
        for (size_t i = 0; i < children_size; ++i)
        {
            key.clear();
            size_t child_capture_offset = capture ? capture->data.size() : 0;
            readStringBinary(key, buf);
            if (capture)
                capture->appendStringBinary(key);

            Node & child = owner.appendChild(node, key);
            DebugPathScope path_scope(merged_json_patch_debug_logging ? &owner.debug_deserialize_path : nullptr, key);
            deserializeNodeExpanded(child, buf, owner, capture);

            if (capture)
            {
                size_t child_capture_end = capture->data.size();
                capture->recordChildSubtree(key, child_capture_offset, child_capture_end - child_capture_offset);
            }
        }

        if (capture)
        {
            auto [subtree, child_subtrees] = capture->finishNode();
            node.serialized_subtree = copyToArena(owner.value_arena, subtree);
            node.serialized_subtree_top_level_children = children_size;
            node.serialized_child_subtrees.clear();
            node.serialized_child_subtrees.reserve(child_subtrees.size());
            for (const auto & child_subtree : child_subtrees)
            {
                SerializedChildSubtree stored_child_subtree;
                stored_child_subtree.name = copyToArena(owner.string_arena, child_subtree.name.view());
                stored_child_subtree.offset = child_subtree.offset;
                stored_child_subtree.size = child_subtree.size;
                node.serialized_child_subtrees.push_back(stored_child_subtree);
            }
            capture->popNode();
        }
    }

    void deserializeNode(Node & node, ReadBuffer & buf)
    {
        node.has_terminal_value = false;
        node.has_terminal_sort_key = false;
        node.has_serialized_subtree = false;
        node.has_subtree_max_sort_key = false;
        node.terminal_value = EncodedField();
        node.children.clear();
        node.serialized_child_subtrees.clear();

        SerializedSubtreeBuilder capture;
        deserializeNodeExpanded(node, buf, *this, &capture);

        constexpr size_t retained_serialized_subtree_min_children = 8;
        constexpr size_t retained_serialized_subtree_min_bytes = 256;

        bool keep_serialized_subtree
            = node.serialized_subtree_top_level_children >= retained_serialized_subtree_min_children
            || node.serialized_subtree.size >= retained_serialized_subtree_min_bytes;

        if (!keep_serialized_subtree)
        {
            clearSerializedSubtree(node);
            return;
        }

        node.has_terminal_value = false;
        node.has_terminal_sort_key = false;
        node.has_serialized_subtree = true;
        node.terminal_value = EncodedField();
        node.children.clear();
        refreshSubtreeMaxSortKey(node, nodes);
    }

    void collectObject(const Node & node, Object & out) const
    {
        if (node.has_serialized_subtree)
        {
            Node expanded = node;
            auto & mutable_self = const_cast<AggregateFunctionMergedJSONPatchData &>(*this);
            ensureExpanded(expanded, mutable_self);
            collectObject(expanded, out);
            return;
        }

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

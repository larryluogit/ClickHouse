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

    struct EncodedField
    {
        StringSlice data;

        EncodedField() = default;

        explicit EncodedField(StringSlice data_)
            : data(data_)
        {
        }

        Field get() const
        {
            ReadBufferFromString buf(data.view());
            return decodeField(buf);
        }
    };

    struct Node
    {
        std::map<String, Node> children;
        std::optional<EncodedField> terminal_value;
        std::optional<SortKey> terminal_sort_key;
    };

    Arena string_arena;
    Arena value_arena;
    Node root;

    AggregateFunctionMergedJSONPatchData() = default;

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
        WriteBufferFromOwnString buf;
        encodeField(value, buf);
        return EncodedField(copyToArena(value_arena, buf.str()));
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

    static void buildFieldFromNode(const Node & node, Field & out)
    {
        if (node.terminal_value)
        {
            out = node.terminal_value->get();
            return;
        }

        Object object;
        for (const auto & [name, child] : node.children)
        {
            Field child_value;
            buildFieldFromNode(child, child_value);
            if (!child_value.isNull())
                object[name] = std::move(child_value);
        }

        out = Field(std::move(object));
    }

    static void mergeObjectIntoNode(Node & node, const Object & object, const SortKey & sort_key, AggregateFunctionMergedJSONPatchData & owner)
    {
        if (node.terminal_sort_key && *node.terminal_sort_key > sort_key)
            return;

        node.terminal_value.reset();
        node.terminal_sort_key.reset();

        for (const auto & [key, value] : object)
            owner.insertPathValue(node, key, value, sort_key);
    }

    void insertField(Node & node, Field value, const SortKey & sort_key)
    {
        normalizeMixedJSONArray(value);

        if (!isObjectField(value))
        {
            node.children.clear();
            node.terminal_value = encodeFieldToArena(std::move(value));
            node.terminal_sort_key = sort_key;
            return;
        }

        if (node.terminal_sort_key && *node.terminal_sort_key > sort_key)
            return;

        node.terminal_value.reset();
        node.terminal_sort_key.reset();

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
            if (current->terminal_sort_key && *current->terminal_sort_key > sort_key)
                return;

            bool is_last = (i + 1 == parts.size());
            auto [it, inserted] = current->children.try_emplace(String(parts[i]), Node{});
            current = &it->second;

            if (is_last)
                insertField(*current, std::move(value), sort_key);
        }
    }

    void insertPathValue(std::string_view path, Field value, const SortKey & sort_key)
    {
        insertPathValue(root, path, std::move(value), sort_key);
    }

    static void mergeNode(Node & dst, const Node & src, AggregateFunctionMergedJSONPatchData & owner)
    {
        if (src.terminal_value && src.terminal_sort_key)
        {
            Field src_value = src.terminal_value->get();
            if (!isObjectField(src_value))
            {
                if (!dst.terminal_sort_key || *dst.terminal_sort_key <= *src.terminal_sort_key)
                {
                    dst.children.clear();
                    dst.terminal_value = owner.encodeFieldToArena(std::move(src_value));
                    dst.terminal_sort_key = *src.terminal_sort_key;
                }
                return;
            }

            if (!dst.terminal_sort_key || *dst.terminal_sort_key <= *src.terminal_sort_key)
                mergeObjectIntoNode(dst, src_value.safeGet<Object>(), *src.terminal_sort_key, owner);
        }

        if (dst.terminal_value && dst.terminal_sort_key)
        {
            Field dst_value = dst.terminal_value->get();
            if (!isObjectField(dst_value))
                return;

            if (src.terminal_sort_key && *src.terminal_sort_key > *dst.terminal_sort_key)
            {
                dst.terminal_value.reset();
                dst.terminal_sort_key.reset();
            }
        }

        for (const auto & [name, src_child] : src.children)
        {
            auto [it, inserted] = dst.children.try_emplace(name, Node{});
            mergeNode(it->second, src_child, owner);
        }
    }

    static void serializeNode(const Node & node, WriteBuffer & buf)
    {
        writeBoolText(node.terminal_value.has_value(), buf);
        if (node.terminal_value)
        {
            writeStringBinary(node.terminal_value->data.view(), buf);
            encodeField(node.terminal_sort_key->toField(), buf);
        }

        writeVarUInt(node.children.size(), buf);
        for (const auto & [name, child] : node.children)
        {
            writeStringBinary(name, buf);
            serializeNode(child, buf);
        }
    }

    void deserializeNode(Node & node, ReadBuffer & buf)
    {
        bool has_terminal = false;
        readBoolText(has_terminal, buf);
        if (has_terminal)
        {
            String value_data;
            readStringBinary(value_data, buf);
            node.terminal_value = EncodedField(copyToArena(value_arena, value_data));
            node.terminal_sort_key = SortKey(decodeField(buf));
        }

        size_t children_size = 0;
        readVarUInt(children_size, buf);
        for (size_t i = 0; i < children_size; ++i)
        {
            String key;
            readStringBinary(key, buf);
            auto [it, inserted] = node.children.try_emplace(key, Node{});
            deserializeNode(it->second, buf);
        }
    }

    static void collectObject(const Node & node, Object & out)
    {
        if (node.terminal_value)
        {
            Field value = node.terminal_value->get();
            if (isObjectField(value))
            {
                const auto & object = value.safeGet<Object>();
                for (const auto & [key, child_value] : object)
                    out[key] = child_value;
            }
            return;
        }

        for (const auto & [name, child] : node.children)
        {
            Field child_value;
            buildFieldFromNode(child, child_value);
            if (!child_value.isNull())
                out[name] = std::move(child_value);
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
        mergeNode(root, other.root, *this);
    }

    void serialize(WriteBuffer & buf) const
    {
        serializeNode(root, buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        root = Node{};
        deserializeNode(root, buf);
    }

    void insertResultInto(IColumn & to, const DataTypePtr &) const
    {
        auto & result_column = assert_cast<ColumnObject &>(to);

        Object result_object;
        collectObject(root, result_object);

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

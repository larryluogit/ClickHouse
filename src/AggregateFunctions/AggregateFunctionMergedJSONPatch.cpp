#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorToString.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
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

    /// Store triplets: (key, value, sorting_key)
    /// Each key only keeps the latest record according to sorting_key.
    /// This implements last-write-wins semantics at the path level, which is the core merge
    /// behavior of RFC 7396 JSON Merge Patch, and enables distributed queries to work correctly
    /// by merging states at the final stage.
    ///
    /// LIMITATION: RFC 7396 null deletion semantics (where `{"key": null}` removes a key) are NOT supported.
    /// When a JSON object like `{"key": null}` is inserted into ColumnObject, the null-valued
    /// key is silently dropped (see ColumnObject::insert() lines 495-504). ColumnObject
    /// cannot distinguish between "key is absent" and "key has null value", treating them
    /// as equivalent. Therefore, this aggregate function cannot detect or handle null deletion.
    struct ValueWithSortKey
    {
        Field value;         /// The JSON value for this path
        Field sort_key;      /// Sorting key to determine which value is latest
        
        bool operator<(const ValueWithSortKey & other) const
        {
            return sort_key < other.sort_key;
        }
    };
    
    /// Map from JSON path (key) to its value and sort key
    UnorderedMapWithMemoryTracking<String, ValueWithSortKey> key_value_map;
    size_t max_dynamic_paths;
    size_t max_dynamic_types;

    /// Default constructor for base class
    AggregateFunctionMergedJSONPatchData()
        : max_dynamic_paths(DataTypeObject::DEFAULT_MAX_DYNAMIC_PATHS)
        , max_dynamic_types(DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES)
    {
    }

    AggregateFunctionMergedJSONPatchData(size_t max_dynamic_paths_, size_t max_dynamic_types_)
        : max_dynamic_paths(max_dynamic_paths_)
        , max_dynamic_types(max_dynamic_types_)
    {
    }

    void add(const IColumn & json_column, size_t row_num, Arena * arena)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        
        /// Use a serialized representation of the object as sort key for deterministic ordering
        const char * begin = nullptr;
        auto serialized = object_column.serializeValueIntoArena(row_num, *arena, begin, nullptr);
        Field sort_key = Field(String(serialized));
        
        /// Extract all key-value pairs from the JSON object
        addKeyValuePairs(object_column, row_num, sort_key);
    }

    void addWithKey(const IColumn & json_column, const IColumn & key_column, size_t row_num, Arena *)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        
        /// Get sort key
        Field sort_key = key_column[row_num];
        
        /// Extract all key-value pairs from the JSON object
        addKeyValuePairs(object_column, row_num, sort_key);
    }

    static bool isPathPrefix(std::string_view prefix, std::string_view path)
    {
        return path.size() > prefix.size()
            && path.starts_with(prefix)
            && path[prefix.size()] == '.';
    }

    void removeConflictingPaths(std::string_view key, const Field & sort_key)
    {
        for (auto it = key_value_map.begin(); it != key_value_map.end();)
        {
            if (isPathPrefix(key, it->first) || isPathPrefix(it->first, key))
            {
                if (it->second.sort_key <= sort_key)
                    it = key_value_map.erase(it);
                else
                    ++it;
            }
            else
            {
                ++it;
            }
        }
    }

    bool hasNewerConflictingPath(std::string_view key, const Field & sort_key) const
    {
        for (const auto & [existing_key, value_with_key] : key_value_map)
        {
            if ((isPathPrefix(key, existing_key) || isPathPrefix(existing_key, key)) && value_with_key.sort_key > sort_key)
                return true;
        }

        return false;
    }

    void upsertPathValue(String key, Field value, const Field & sort_key)
    {
        auto map_it = key_value_map.find(key);
        if (map_it != key_value_map.end() && map_it->second.sort_key > sort_key)
            return;

        if (hasNewerConflictingPath(key, sort_key))
            return;

        removeConflictingPaths(key, sort_key);
        key_value_map.insert_or_assign(std::move(key), ValueWithSortKey{std::move(value), sort_key});
    }

    void addKeyValuePairs(const ColumnObject & object_column, size_t row_num, const Field & sort_key)
    {
        /// Use SortedPathsIterator to iterate over all paths (typed + dynamic + shared data)
        ColumnObject::SortedPathsIterator it(object_column, row_num);
        while (!it.end())
        {
            auto path_info = it.getCurrentPathInfo();
            String key(path_info.path);

            /// Get the value for this path
            Field value;
            path_info.column->get(path_info.row, value);
            normalizeMixedJSONArray(value);

            upsertPathValue(std::move(key), std::move(value), sort_key);

            it.next();
        }
    }

    void merge(const AggregateFunctionMergedJSONPatchData & other, Arena *)
    {
        /// Merge key-value pairs from other state, keeping only the latest for each key.
        /// This implements last-write-wins semantics (RFC 7396 core behavior): for each path,
        /// the value with the largest sorting_key wins, which is equivalent to merging objects
        /// in sorted order. Note: RFC 7396 null deletion is not supported (see limitation above).
        for (const auto & [key, value_with_key] : other.key_value_map)
            upsertPathValue(key, value_with_key.value, value_with_key.sort_key);
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = key_value_map.size();
        writeVarUInt(size, buf);
        
        for (const auto & [key, value_with_key] : key_value_map)
        {
            /// Serialize key (path)
            writeStringBinary(key, buf);
            
            /// Serialize value
            writeFieldBinary(value_with_key.value, buf);
            
            /// Serialize sort key
            writeFieldBinary(value_with_key.sort_key, buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        key_value_map.reserve(size);
        
        for (size_t i = 0; i < size; ++i)
        {
            /// Deserialize key (path)
            String key;
            readStringBinary(key, buf);
            
            /// Deserialize value
            Field value = readFieldBinary(buf);
            
            /// Deserialize sort key
            Field sort_key = readFieldBinary(buf);
            
            key_value_map.emplace(key, ValueWithSortKey{value, sort_key});
        }
    }

    void insertResultInto(IColumn & to, const DataTypePtr &) const
    {
        auto & result_column = assert_cast<ColumnObject &>(to);

        if (key_value_map.empty())
        {
            /// Insert default value (empty JSON object)
            result_column.insertDefault();
            return;
        }

        Object result_object;
        for (const auto & [key, value_with_key] : key_value_map)
        {
            if (value_with_key.value.isNull())
                continue;

            result_object[key] = value_with_key.value;
        }

        try
        {
            result_column.insert(Field(result_object));
        }
        catch (Exception & e)
        {
            String sample;
            size_t count = 0;
            for (const auto & [key, value_with_key] : key_value_map)
            {
                if (count >= 16)
                    break;

                if (!sample.empty())
                    sample += ", ";

                sample += key;
                sample += "=";
                sample += applyVisitor(FieldVisitorToString(), value_with_key.value);
                ++count;
            }

            e.addMessage(fmt::format(
                "Debug `mergedJSONPatch`: failed to insert finalized JSON object with {} stored paths. Sample paths: {}",
                key_value_map.size(),
                sample));
            throw;
        }
    }
};


class AggregateFunctionMergedJSONPatch final
    : public IAggregateFunctionDataHelper<AggregateFunctionMergedJSONPatchData, AggregateFunctionMergedJSONPatch>
{
private:
    DataTypePtr json_type;
    DataTypePtr key_type;  /// nullptr if no sort key
    bool has_sort_key;

public:
    explicit AggregateFunctionMergedJSONPatch(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionMergedJSONPatchData, AggregateFunctionMergedJSONPatch>(
            argument_types_, {}, argument_types_[0])
        , json_type(argument_types_[0])
        , key_type(argument_types_.size() > 1 ? argument_types_[1] : nullptr)
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

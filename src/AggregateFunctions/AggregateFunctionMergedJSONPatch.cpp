#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/PODArray.h>
#include <Common/Arena.h>
#include <Core/Field.h>
#include <base/sort.h>
#include <numeric>


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
    /// Store ColumnObject instances paired with their sort keys
    struct ObjectWithKey
    {
        MutableColumnPtr object_column;  /// Stores single ColumnObject row
        Field sort_key;
        
        bool operator<(const ObjectWithKey & other) const
        {
            return sort_key < other.sort_key;
        }
    };
    
    std::vector<ObjectWithKey> values;
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
        
        /// Create a single-row ColumnObject by extracting the row
        auto single_row_column = object_column.cloneEmpty();
        single_row_column->insertFrom(object_column, row_num);
        
        /// Use a serialized representation of the object as sort key for deterministic ordering
        const char * begin = nullptr;
        auto serialized = object_column.serializeValueIntoArena(row_num, *arena, begin, nullptr);
        
        values.emplace_back(ObjectWithKey{std::move(single_row_column), Field(String(serialized))});
    }

    void addWithKey(const IColumn & json_column, const IColumn & key_column, size_t row_num, Arena *)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        
        /// Create a single-row ColumnObject
        auto single_row_column = object_column.cloneEmpty();
        single_row_column->insertFrom(object_column, row_num);
        
        /// Get sort key
        Field sort_key = key_column[row_num];
        
        values.emplace_back(ObjectWithKey{std::move(single_row_column), sort_key});
    }

    void merge(const AggregateFunctionMergedJSONPatchData & other, Arena *)
    {
        /// Concatenate all values from other state
        for (const auto & item : other.values)
        {
            /// Clone the column to avoid shared ownership issues
            values.emplace_back(ObjectWithKey{item.object_column->cloneResized(item.object_column->size()), item.sort_key});
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = values.size();
        writeVarUInt(size, buf);
        for (const auto & item : values)
        {
            /// Serialize the ColumnObject
            const auto & object_col = assert_cast<const ColumnObject &>(*item.object_column);
            Arena arena;
            const char * begin = nullptr;
            auto serialized = object_col.serializeValueIntoArena(0, arena, begin, nullptr);
            writeStringBinary(serialized, buf);
            
            /// Serialize sort key
            writeFieldBinary(item.sort_key, buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        values.reserve(size);
        
        for (size_t i = 0; i < size; ++i)
        {
            /// Deserialize ColumnObject
            String serialized_data;
            readStringBinary(serialized_data, buf);
            
            auto column = ColumnObject::create({}, max_dynamic_paths, max_dynamic_types);
            ReadBufferFromString read_buf(serialized_data);
            column->deserializeAndInsertFromArena(read_buf, nullptr);
            
            /// Deserialize sort key
            Field sort_key = readFieldBinary(buf);
            
            values.emplace_back(ObjectWithKey{std::move(column), sort_key});
        }
    }

    void insertResultInto(IColumn & to, const DataTypePtr &) const
    {
        auto & result_column = assert_cast<ColumnObject &>(to);
        
        if (values.empty())
        {
            /// Insert default value (empty JSON object)
            result_column.insertDefault();
            return;
        }

        /// Optimization: If only one value, avoid sorting and merging
        if (values.size() == 1)
        {
            result_column.insertFrom(*values[0].object_column, 0);
            return;
        }

        /// Sort values by sort key to ensure deterministic order
        /// Optimization: Use indices instead of cloning columns during sort
        std::vector<size_t> indices(values.size());
        std::iota(indices.begin(), indices.end(), 0);
        ::sort(indices.begin(), indices.end(), [this](size_t a, size_t b) {
            return values[a].sort_key < values[b].sort_key;
        });

        /// Merge all JSON objects in sorted order using RFC 7396 JSON Merge Patch semantics
        /// Optimization: Start with a clone of the first object to avoid unnecessary allocation
        auto merged_column = values[indices[0]].object_column->cloneResized(1);
        auto & merged_object = assert_cast<ColumnObject &>(*merged_column);
        
        /// Merge subsequent objects using ColumnObject::mergeObjectColumns
        for (size_t i = 1; i < indices.size(); ++i)
        {
            const auto & source_object = assert_cast<const ColumnObject &>(*values[indices[i]].object_column);
            ColumnObject::mergeObjectColumns(merged_object, source_object, 0, 0);
        }
        
        /// Insert the merged result
        result_column.insertFrom(*merged_column, 0);
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
            this->data(place).addWithKey(*columns[0], *columns[1], row_num, arena);
        else
            this->data(place).add(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to, result_type);
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
Aggregates JSON values by merging them using RFC 7396 JSON Merge Patch algorithm.

When called with one argument `mergedJSONPatch(json_col)`, all JSON objects are collected from all shards
and sorted lexicographically to ensure deterministic order across distributed queries.

When called with two arguments `mergedJSONPatch(json_col, sort_key)`, JSON objects are sorted by the sort_key
before merging, allowing explicit control over merge order even in distributed queries.

All JSON objects are collected from all shards, sorted (by sort_key if provided, otherwise lexicographically),
and then merged sequentially. Later values in the sorted order overwrite earlier ones for the same keys.
This ensures consistent results in distributed queries regardless of shard processing order.
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
            "Basic usage",
            R"(
SELECT mergedJSONPatch(json) FROM 
(
    SELECT '{"a":1}'::JSON AS json
    UNION ALL
    SELECT '{"b":2}'::JSON
    UNION ALL
    SELECT '{"a":3, "c":4}'::JSON
);
            )",
            R"(
┌─mergedJSONPatch(json)─┐
│ {"a":3,"b":2,"c":4}   │
└───────────────────────┘
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

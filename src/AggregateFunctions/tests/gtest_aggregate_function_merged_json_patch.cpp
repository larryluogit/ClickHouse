#include <gtest/gtest.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/Arena.h>
#include <Common/tests/gtest_global_register.h>
#include <base/defines.h>

#include <numeric>

/// Unit tests for AggregateFunctionMergedJSONPatch with triplet state storage
///
/// This function stores state as triplets (key, value, sorting_key) to enable
/// distributed queries to work correctly by merging states at the final stage.
///
/// LIMITATION: RFC 7396 null deletion semantics are NOT supported.
/// When a JSON object like {"key": null} is inserted into ColumnObject, the null-valued
/// key is silently dropped (see ColumnObject::insert() lines 495-504). ColumnObject
/// cannot distinguish between "key is absent" and "key has null value", treating them
/// as equivalent. Therefore, this aggregate function cannot detect or handle null deletion.

using namespace DB;

namespace
{

Object createObject(const std::map<String, Field> & path_values)
{
    Object object;
    for (const auto & [path, value] : path_values)
        object[path] = value;
    return object;
}

Int64 getInt64FromObject(const Object & obj, const String & path, Int64 default_value = 0)
{
    auto it = obj.find(path);
    return (it == obj.end()) ? default_value : it->second.safeGet<Int64>();
}

String getStringFromObject(const Object & obj, const String & path, const String & default_value = "")
{
    auto it = obj.find(path);
    return (it == obj.end()) ? default_value : it->second.safeGet<String>();
}

AggregateFunctionPtr createMergedJSONPatchFunction(bool with_sort_key = false)
{
    AggregateFunctionFactory & factory = AggregateFunctionFactory::instance();
    DataTypes argument_types;
    argument_types.push_back(std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));
    if (with_sort_key)
        argument_types.push_back(std::make_shared<DataTypeInt64>());
    Array parameters;
    AggregateFunctionProperties properties;
    return factory.get("mergedJSONPatch", NullsAction::EMPTY, argument_types, parameters, properties);
}

struct StateSizeMetrics
{
    size_t allocated_bytes = 0;
    size_t serialized_bytes = 0;
};

StateSizeMetrics measureStateSize(
    const AggregateFunctionPtr & func,
    const std::vector<std::map<String, Field>> & rows,
    const std::vector<Int64> & sort_keys)
{
    EXPECT_EQ(rows.size(), sort_keys.size());

    PODArray<char> place_buffer;
    place_buffer.resize(func->sizeOfData());
    AggregateDataPtr place = place_buffer.data();
    func->create(place);

    Arena arena;
    auto json_column = ColumnObject::create({}, 1024, 255);
    auto & obj_col = assert_cast<ColumnObject &>(*json_column);
    auto sort_key_column = DataTypeInt64().createColumn();

    for (size_t i = 0; i < rows.size(); ++i)
    {
        obj_col.insert(Field(createObject(rows[i])));
        sort_key_column->insert(Field(sort_keys[i]));
    }

    const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
    for (size_t i = 0; i < rows.size(); ++i)
        func->add(place, columns, i, &arena);

    WriteBufferFromOwnString write_buf;
    func->serialize(place, write_buf, std::nullopt);

    StateSizeMetrics metrics;
    metrics.allocated_bytes = write_buf.str().size();
    metrics.serialized_bytes = write_buf.str().size();

    func->destroy(place);
    return metrics;
}

std::vector<std::map<String, Field>> makeRepeatedUpdatesDataset(size_t updates)
{
    std::vector<std::map<String, Field>> rows;
    rows.reserve(updates);

    for (size_t i = 0; i < updates; ++i)
    {
        rows.push_back({
            {"a", Field(Int64(i))},
            {"name", Field("stable")}
        });
    }

    return rows;
}

std::vector<std::map<String, Field>> makeWideDependencyDataset(size_t dependency_count)
{
    std::map<String, Field> row;
    row["data.runtime.nodejs.name"] = Field("node-service");

    for (size_t i = 0; i < dependency_count; ++i)
        row["data.runtime.nodejs.dependencies.dep" + toString(i)] = Field("1.0." + toString(i));

    return {row};
}

std::vector<Int64> makeIncreasingSortKeys(size_t size)
{
    std::vector<Int64> sort_keys(size);
    std::iota(sort_keys.begin(), sort_keys.end(), 1);
    return sort_keys;
}

std::vector<std::map<String, Field>> makeSharedStructureDataset(size_t rows, size_t dependency_count)
{
    std::vector<std::map<String, Field>> result;
    result.reserve(rows);

    for (size_t row = 0; row < rows; ++row)
    {
        std::map<String, Field> entry;
        entry["data.runtime.nodejs.name"] = Field("node-service");
        entry["data.runtime.nodejs.version"] = Field("20." + toString(row % 3));
        entry["data.runtime.nodejs.region"] = Field("us-east-" + toString(row % 2));
        entry["data.runtime.nodejs.env"] = Field("prod");

        for (size_t dep = 0; dep < dependency_count; ++dep)
            entry["data.runtime.nodejs.dependencies.dep" + toString(dep)] = Field("1." + toString(row) + "." + toString(dep));

        result.push_back(std::move(entry));
    }

    return result;
}

String serializeState(
    const AggregateFunctionPtr & func,
    const std::vector<std::map<String, Field>> & rows,
    const std::vector<Int64> & sort_keys)
{
    EXPECT_EQ(rows.size(), sort_keys.size());

    PODArray<char> place_buffer;
    place_buffer.resize(func->sizeOfData());
    AggregateDataPtr place = place_buffer.data();
    func->create(place);

    Arena arena;
    auto json_column = ColumnObject::create({}, 1024, 255);
    auto & obj_col = assert_cast<ColumnObject &>(*json_column);
    auto sort_key_column = DataTypeInt64().createColumn();

    for (size_t i = 0; i < rows.size(); ++i)
    {
        obj_col.insert(Field(createObject(rows[i])));
        sort_key_column->insert(Field(sort_keys[i]));
    }

    const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
    for (size_t i = 0; i < rows.size(); ++i)
        func->add(place, columns, i, &arena);

    WriteBufferFromOwnString write_buf;
    func->serialize(place, write_buf, std::nullopt);

    func->destroy(place);
    return write_buf.str();
}

}

TEST(AggregateFunctionMergedJSONPatch, OverlappingPathsWithSortKey)
{
    tryRegisterAggregateFunctions();
    
    auto func = createMergedJSONPatchFunction(true);
    PODArray<char> place_buffer;
    place_buffer.resize(func->sizeOfData());
    AggregateDataPtr place = place_buffer.data();
    func->create(place);
    
    Arena arena;
    auto json_column = ColumnObject::create({}, 1024, 255);
    auto & obj_col = assert_cast<ColumnObject &>(*json_column);
    auto sort_key_column = DataTypeInt64().createColumn();
    
    obj_col.insert(Field(createObject({{"a", Field(1)}, {"b", Field(2)}})));
    sort_key_column->insert(Field(Int64(1)));
    obj_col.insert(Field(createObject({{"b", Field(20)}, {"c", Field(3)}})));
    sort_key_column->insert(Field(Int64(2)));
    
    const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
    for (size_t i = 0; i < 2; ++i)
        func->add(place, columns, i, &arena);
    
    auto result_column = func->getResultType()->createColumn();
    func->insertResultInto(place, *result_column, &arena);
    
    Field result_field;
    result_column->get(0, result_field);
    const auto & result_obj = result_field.safeGet<Object>();
    
    EXPECT_EQ(getInt64FromObject(result_obj, "a"), 1);
    EXPECT_EQ(getInt64FromObject(result_obj, "b"), 20);
    EXPECT_EQ(getInt64FromObject(result_obj, "c"), 3);
    
    func->destroy(place);
}

TEST(AggregateFunctionMergedJSONPatch, DistributedQueryMerge)
{
    auto func = createMergedJSONPatchFunction(true);
    PODArray<char> place1_buffer;
    PODArray<char> place2_buffer;
    place1_buffer.resize(func->sizeOfData());
    place2_buffer.resize(func->sizeOfData());
    AggregateDataPtr place1 = place1_buffer.data();
    AggregateDataPtr place2 = place2_buffer.data();
    func->create(place1);
    func->create(place2);
    
    Arena arena;
    
    {
        auto json_column = ColumnObject::create({}, 1024, 255);
        auto & obj_col = assert_cast<ColumnObject &>(*json_column);
        auto sort_key_column = DataTypeInt64().createColumn();
        
        obj_col.insert(Field(createObject({{"user", Field("Alice")}, {"age", Field(25)}})));
        sort_key_column->insert(Field(Int64(1)));
        obj_col.insert(Field(createObject({{"user", Field("Bob")}, {"age", Field(30)}})));
        sort_key_column->insert(Field(Int64(2)));
        
        const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
        for (size_t i = 0; i < 2; ++i)
            func->add(place1, columns, i, &arena);
    }
    
    {
        auto json_column = ColumnObject::create({}, 1024, 255);
        auto & obj_col = assert_cast<ColumnObject &>(*json_column);
        auto sort_key_column = DataTypeInt64().createColumn();
        
        obj_col.insert(Field(createObject({{"user", Field("Charlie")}, {"age", Field(35)}})));
        sort_key_column->insert(Field(Int64(3)));
        obj_col.insert(Field(createObject({{"user", Field("Alice")}, {"age", Field(26)}})));
        sort_key_column->insert(Field(Int64(4)));
        
        const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
        for (size_t i = 0; i < 2; ++i)
            func->add(place2, columns, i, &arena);
    }
    
    func->merge(place1, place2, &arena);
    
    auto result_column = func->getResultType()->createColumn();
    func->insertResultInto(place1, *result_column, &arena);
    
    Field result_field;
    result_column->get(0, result_field);
    const auto & result_obj = result_field.safeGet<Object>();
    
    EXPECT_EQ(getStringFromObject(result_obj, "user"), "Alice");
    EXPECT_EQ(getInt64FromObject(result_obj, "age"), 26);
    
    func->destroy(place1);
    func->destroy(place2);
}

TEST(AggregateFunctionMergedJSONPatch, SerializeDeserialize)
{
    tryRegisterAggregateFunctions();
    
    auto func = createMergedJSONPatchFunction(true);
    PODArray<char> place1_buffer;
    place1_buffer.resize(func->sizeOfData());
    AggregateDataPtr place1 = place1_buffer.data();
    func->create(place1);
    
    Arena arena;
    auto json_column = ColumnObject::create({}, 1024, 255);
    auto & obj_col = assert_cast<ColumnObject &>(*json_column);
    auto sort_key_column = DataTypeInt64().createColumn();
    
    obj_col.insert(Field(createObject({{"a", Field(1)}, {"b", Field(2)}})));
    sort_key_column->insert(Field(Int64(1)));
    obj_col.insert(Field(createObject({{"a", Field(3)}, {"c", Field(4)}})));
    sort_key_column->insert(Field(Int64(2)));
    
    const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
    for (size_t i = 0; i < 2; ++i)
        func->add(place1, columns, i, &arena);
    
    WriteBufferFromOwnString write_buf;
    func->serialize(place1, write_buf, std::nullopt);
    String serialized = write_buf.str();
    
    PODArray<char> place2_buffer;
    place2_buffer.resize(func->sizeOfData());
    AggregateDataPtr place2 = place2_buffer.data();
    func->create(place2);
    
    ReadBufferFromString read_buf(serialized);
    func->deserialize(place2, read_buf, std::nullopt, &arena);
    
    auto result1_column = func->getResultType()->createColumn();
    auto result2_column = func->getResultType()->createColumn();
    func->insertResultInto(place1, *result1_column, &arena);
    func->insertResultInto(place2, *result2_column, &arena);
    
    Field result1_field;
    Field result2_field;
    result1_column->get(0, result1_field);
    result2_column->get(0, result2_field);
    
    const auto & result1_obj = result1_field.safeGet<Object>();
    const auto & result2_obj = result2_field.safeGet<Object>();
    
    EXPECT_EQ(result1_obj.size(), result2_obj.size());
    EXPECT_EQ(getInt64FromObject(result1_obj, "a"), getInt64FromObject(result2_obj, "a"));
    EXPECT_EQ(getInt64FromObject(result1_obj, "b"), getInt64FromObject(result2_obj, "b"));
    EXPECT_EQ(getInt64FromObject(result1_obj, "c"), getInt64FromObject(result2_obj, "c"));
    
    func->destroy(place1);
    func->destroy(place2);
}

TEST(AggregateFunctionMergedJSONPatch, ParentPathReplacesEarlierDescendants)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);
    PODArray<char> place_buffer;
    place_buffer.resize(func->sizeOfData());
    AggregateDataPtr place = place_buffer.data();
    func->create(place);

    Arena arena;
    auto json_column = ColumnObject::create({}, 1024, 255);
    auto & obj_col = assert_cast<ColumnObject &>(*json_column);
    auto sort_key_column = DataTypeInt64().createColumn();

    obj_col.insert(Field(createObject({{"a.nested", Field(Int64(1))}})));
    sort_key_column->insert(Field(Int64(1)));
    obj_col.insert(Field(createObject({{"a", Field("plain text")}})));
    sort_key_column->insert(Field(Int64(2)));

    const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
    for (size_t i = 0; i < 2; ++i)
        func->add(place, columns, i, &arena);

    auto result_column = func->getResultType()->createColumn();
    func->insertResultInto(place, *result_column, &arena);

    Field result_field;
    result_column->get(0, result_field);
    const auto & result_obj = result_field.safeGet<Object>();

    ASSERT_TRUE(result_obj.contains("a"));
    EXPECT_EQ(result_obj.at("a").safeGet<String>(), "plain text");
    EXPECT_FALSE(result_obj.contains("a.nested"));

    func->destroy(place);
}
TEST(AggregateFunctionMergedJSONPatch, NewerAncestorWinsDuringStateMerge)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);

    PODArray<char> place1_buffer;
    PODArray<char> place2_buffer;
    place1_buffer.resize(func->sizeOfData());
    place2_buffer.resize(func->sizeOfData());
    AggregateDataPtr place1 = place1_buffer.data();
    AggregateDataPtr place2 = place2_buffer.data();
    func->create(place1);
    func->create(place2);

    Arena arena;

    {
        auto json_column = ColumnObject::create({}, 1024, 255);
        auto & obj_col = assert_cast<ColumnObject &>(*json_column);
        auto sort_key_column = DataTypeInt64().createColumn();

        obj_col.insert(Field(createObject({{"a.nested", Field(Int64(1))}})));
        sort_key_column->insert(Field(Int64(1)));

        const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
        func->add(place1, columns, 0, &arena);
    }

    {
        auto json_column = ColumnObject::create({}, 1024, 255);
        auto & obj_col = assert_cast<ColumnObject &>(*json_column);
        auto sort_key_column = DataTypeInt64().createColumn();

        obj_col.insert(Field(createObject({{"a", Field("plain text")}})));
        sort_key_column->insert(Field(Int64(2)));

        const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
        func->add(place2, columns, 0, &arena);
    }

    func->merge(place1, place2, &arena);

    auto result_column = func->getResultType()->createColumn();
    func->insertResultInto(place1, *result_column, &arena);

    Field result_field;
    result_column->get(0, result_field);
    const auto & result_obj = result_field.safeGet<Object>();

    ASSERT_TRUE(result_obj.contains("a"));
    EXPECT_EQ(result_obj.at("a").safeGet<String>(), "plain text");
    EXPECT_FALSE(result_obj.contains("a.nested"));

    func->destroy(place1);
    func->destroy(place2);
}

TEST(AggregateFunctionMergedJSONPatch, StateMemoryConsumptionSharedStructureMetrics)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);

    auto metrics = measureStateSize(
        func,
        makeSharedStructureDataset(/* rows */ 20, /* dependency_count */ 64),
        makeIncreasingSortKeys(20));

    EXPECT_GT(metrics.serialized_bytes, 0U);

    std::cerr << "mergedJSONPatch shared-structure metrics: allocated=" << metrics.allocated_bytes
              << " serialized=" << metrics.serialized_bytes << std::endl;
}

TEST(AggregateFunctionMergedJSONPatch, RepeatedDeserializeOfLargeSharedStructureState)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);
    String serialized = serializeState(
        func,
        makeSharedStructureDataset(/* rows */ 20, /* dependency_count */ 128),
        makeIncreasingSortKeys(20));

    for (size_t iteration = 0; iteration < 32; ++iteration)
    {
        PODArray<char> place_buffer;
        place_buffer.resize(func->sizeOfData());
        AggregateDataPtr place = place_buffer.data();
        func->create(place);

        Arena arena;
        ReadBufferFromString read_buf(serialized);
        ASSERT_NO_THROW(func->deserialize(place, read_buf, std::nullopt, &arena));

        auto result_column = func->getResultType()->createColumn();
        ASSERT_NO_THROW(func->insertResultInto(place, *result_column, &arena));

        Field result_field;
        result_column->get(0, result_field);
        const auto & result_obj = result_field.safeGet<Object>();

        ASSERT_TRUE(result_obj.contains("data"));
        const auto & data_obj = result_obj.at("data").safeGet<Object>();
        ASSERT_TRUE(data_obj.contains("runtime"));
        const auto & runtime_obj = data_obj.at("runtime").safeGet<Object>();
        ASSERT_TRUE(runtime_obj.contains("nodejs"));
        const auto & nodejs_obj = runtime_obj.at("nodejs").safeGet<Object>();
        ASSERT_TRUE(nodejs_obj.contains("name"));
        EXPECT_EQ(nodejs_obj.at("name").safeGet<String>(), "node-service");
        ASSERT_TRUE(nodejs_obj.contains("env"));
        EXPECT_EQ(nodejs_obj.at("env").safeGet<String>(), "prod");

        func->destroy(place);
    }
}

TEST(AggregateFunctionMergedJSONPatch, MixedScalarAndObjectArrayElementsAreSanitizedDuringAggregation)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);

    PODArray<char> place_buffer;
    place_buffer.resize(func->sizeOfData());
    AggregateDataPtr place = place_buffer.data();
    func->create(place);

    Arena arena;
    auto sort_key_column = DataTypeInt64().createColumn();

    {
        auto source_json_column = ColumnObject::create({}, 1, 255);
        auto & source_obj_col = assert_cast<ColumnObject &>(*source_json_column);

        Object sanitized_source;
        sanitized_source["capabilities"] = Array{
            Field("gitops"),
            Field("logdownload"),
            Field(R"({"java-trace-commands":["jstack","jmap"]})"),
            Field("log4j-safe-lib")
        };
        sanitized_source["name"] = Field("agent-b");

        source_obj_col.insert(Field(sanitized_source));
        sort_key_column->insert(Field(Int64(2)));

        const IColumn * columns[] = {source_json_column.get(), sort_key_column.get()};
        func->add(place, columns, 0, &arena);
    }

    auto result_column = func->getResultType()->createColumn();
    ASSERT_NO_THROW(func->insertResultInto(place, *result_column, &arena));

    Field result_field;
    result_column->get(0, result_field);
    const auto & result_obj = result_field.safeGet<Object>();

    ASSERT_TRUE(result_obj.contains("capabilities"));
    ASSERT_TRUE(result_obj.contains("name"));
    EXPECT_EQ(result_obj.at("name").safeGet<String>(), "agent-b");

    const auto & capabilities = result_obj.at("capabilities").safeGet<Array>();
    ASSERT_EQ(capabilities.size(), 4);
    EXPECT_EQ(capabilities[0].safeGet<String>(), "gitops");
    EXPECT_EQ(capabilities[1].safeGet<String>(), "logdownload");
    EXPECT_EQ(capabilities[2].safeGet<String>(), R"({"java-trace-commands":["jstack","jmap"]})");
    EXPECT_EQ(capabilities[3].safeGet<String>(), "log4j-safe-lib");

    func->destroy(place);
}

TEST(AggregateFunctionMergedJSONPatch, SerializedStatePreservesConflictResolutionOnMergeAndDeserialize)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);

    PODArray<char> place1_buffer;
    PODArray<char> place2_buffer;
    PODArray<char> place3_buffer;
    place1_buffer.resize(func->sizeOfData());
    place2_buffer.resize(func->sizeOfData());
    place3_buffer.resize(func->sizeOfData());

    AggregateDataPtr place1 = place1_buffer.data();
    AggregateDataPtr place2 = place2_buffer.data();
    AggregateDataPtr place3 = place3_buffer.data();
    func->create(place1);
    func->create(place2);
    func->create(place3);

    Arena arena;

    {
        auto json_column = ColumnObject::create({}, 1024, 255);
        auto & obj_col = assert_cast<ColumnObject &>(*json_column);
        auto sort_key_column = DataTypeInt64().createColumn();

        obj_col.insert(Field(createObject({
            {"a.nested", Field(Int64(1))},
            {"caps", Array{Field("alpha"), Field("beta")}}
        })));
        sort_key_column->insert(Field(Int64(1)));

        const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
        func->add(place1, columns, 0, &arena);
    }

    {
        auto json_column = ColumnObject::create({}, 1024, 255);
        auto & obj_col = assert_cast<ColumnObject &>(*json_column);
        auto sort_key_column = DataTypeInt64().createColumn();

        obj_col.insert(Field(createObject({
            {"a", Field("plain text")},
            {"caps", Array{Field("newer")}},
            {"name", Field("agent-x")}
        })));
        sort_key_column->insert(Field(Int64(2)));

        const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
        func->add(place2, columns, 0, &arena);
    }

    WriteBufferFromOwnString write_buf;
    func->serialize(place2, write_buf, std::nullopt);
    ReadBufferFromString read_buf(write_buf.str());
    func->deserialize(place3, read_buf, std::nullopt, &arena);

    func->merge(place1, place3, &arena);

    auto result_column = func->getResultType()->createColumn();
    func->insertResultInto(place1, *result_column, &arena);

    Field result_field;
    result_column->get(0, result_field);
    const auto & result_obj = result_field.safeGet<Object>();

    ASSERT_TRUE(result_obj.contains("a"));
    EXPECT_EQ(result_obj.at("a").safeGet<String>(), "plain text");
    EXPECT_FALSE(result_obj.contains("a.nested"));
    ASSERT_TRUE(result_obj.contains("caps"));
    ASSERT_TRUE(result_obj.contains("name"));
    EXPECT_EQ(result_obj.at("name").safeGet<String>(), "agent-x");

    const auto & caps = result_obj.at("caps").safeGet<Array>();
    ASSERT_EQ(caps.size(), 1);
    EXPECT_EQ(caps[0].safeGet<String>(), "newer");

    func->destroy(place1);
    func->destroy(place2);
    func->destroy(place3);
}

TEST(AggregateFunctionMergedJSONPatch, RepeatedSamePathUpdatesDoNotAccumulateState)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);

    PODArray<char> place_buffer;
    place_buffer.resize(func->sizeOfData());
    AggregateDataPtr place = place_buffer.data();
    func->create(place);

    Arena arena;
    auto json_column = ColumnObject::create({}, 1024, 255);
    auto & obj_col = assert_cast<ColumnObject &>(*json_column);
    auto sort_key_column = DataTypeInt64().createColumn();

    for (Int64 i = 0; i < 64; ++i)
    {
        obj_col.insert(Field(createObject({
            {"a", Field(i)},
            {"name", Field("stable")}
        })));
        sort_key_column->insert(Field(i));
    }

    const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
    for (size_t i = 0; i < 64; ++i)
        func->add(place, columns, i, &arena);

    WriteBufferFromOwnString write_buf;
    func->serialize(place, write_buf, std::nullopt);

    auto result_column = func->getResultType()->createColumn();
    func->insertResultInto(place, *result_column, &arena);

    Field result_field;
    result_column->get(0, result_field);
    const auto & result_obj = result_field.safeGet<Object>();

    EXPECT_EQ(getInt64FromObject(result_obj, "a"), 63);
    EXPECT_EQ(getStringFromObject(result_obj, "name"), "stable");

    ReadBufferFromString read_buf(write_buf.str());
    bool has_terminal = false;
    readBoolText(has_terminal, read_buf);
    EXPECT_FALSE(has_terminal);

    size_t root_children = 0;
    readVarUInt(root_children, read_buf);
    EXPECT_EQ(root_children, 2);

    func->destroy(place);
}

TEST(AggregateFunctionMergedJSONPatch, MergeOfCompactedStatesKeepsOnlyWinningEntries)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);

    PODArray<char> left_buffer;
    PODArray<char> right_buffer;
    left_buffer.resize(func->sizeOfData());
    right_buffer.resize(func->sizeOfData());
    AggregateDataPtr left = left_buffer.data();
    AggregateDataPtr right = right_buffer.data();
    func->create(left);
    func->create(right);

    Arena arena;

    {
        auto json_column = ColumnObject::create({}, 1024, 255);
        auto & obj_col = assert_cast<ColumnObject &>(*json_column);
        auto sort_key_column = DataTypeInt64().createColumn();

        obj_col.insert(Field(createObject({
            {"root.child", Field(Int64(1))},
            {"keep", Field("left")}
        })));
        sort_key_column->insert(Field(Int64(1)));

        obj_col.insert(Field(createObject({
            {"root.child", Field(Int64(2))},
            {"keep", Field("left-new")}
        })));
        sort_key_column->insert(Field(Int64(2)));

        const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
        func->add(left, columns, 0, &arena);
        func->add(left, columns, 1, &arena);
    }

    {
        auto json_column = ColumnObject::create({}, 1024, 255);
        auto & obj_col = assert_cast<ColumnObject &>(*json_column);
        auto sort_key_column = DataTypeInt64().createColumn();

        obj_col.insert(Field(createObject({
            {"root", Field("winner")},
            {"other", Field("value")}
        })));
        sort_key_column->insert(Field(Int64(3)));

        const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
        func->add(right, columns, 0, &arena);
    }

    WriteBufferFromOwnString left_buf;
    WriteBufferFromOwnString right_buf;
    func->serialize(left, left_buf, std::nullopt);
    func->serialize(right, right_buf, std::nullopt);

    func->merge(left, right, &arena);

    auto result_column = func->getResultType()->createColumn();
    func->insertResultInto(left, *result_column, &arena);

    Field result_field;
    result_column->get(0, result_field);
    const auto & result_obj = result_field.safeGet<Object>();

    ASSERT_TRUE(result_obj.contains("root"));
    EXPECT_EQ(result_obj.at("root").safeGet<String>(), "winner");
    EXPECT_FALSE(result_obj.contains("root.child"));
    EXPECT_EQ(getStringFromObject(result_obj, "keep"), "left-new");
    EXPECT_EQ(getStringFromObject(result_obj, "other"), "value");

    ReadBufferFromString read_left(left_buf.str());
    ReadBufferFromString read_right(right_buf.str());

    bool left_has_terminal = false;
    bool right_has_terminal = false;
    readBoolText(left_has_terminal, read_left);
    readBoolText(right_has_terminal, read_right);
    EXPECT_FALSE(left_has_terminal);
    EXPECT_FALSE(right_has_terminal);

    size_t left_children = 0;
    size_t right_children = 0;
    readVarUInt(left_children, read_left);
    readVarUInt(right_children, read_right);

    EXPECT_EQ(left_children, 2);
    EXPECT_EQ(right_children, 2);

    WriteBufferFromOwnString merged_buf;
    func->serialize(left, merged_buf, std::nullopt);
    ReadBufferFromString merged_read(merged_buf.str());

    bool merged_has_terminal = false;
    readBoolText(merged_has_terminal, merged_read);
    EXPECT_FALSE(merged_has_terminal);

    size_t merged_children = 0;
    readVarUInt(merged_children, merged_read);
    EXPECT_EQ(merged_children, 3);

    func->destroy(left);
    func->destroy(right);
}

TEST(AggregateFunctionMergedJSONPatch, WideSubtreeSerializesAsNestedTree)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);

    PODArray<char> place_buffer;
    place_buffer.resize(func->sizeOfData());
    AggregateDataPtr place = place_buffer.data();
    func->create(place);

    Arena arena;
    auto json_column = ColumnObject::create({}, 1024, 255);
    auto & obj_col = assert_cast<ColumnObject &>(*json_column);
    auto sort_key_column = DataTypeInt64().createColumn();

    obj_col.insert(Field(createObject({
        {"data.runtime.nodejs.dependencies.react", Field("18.0.0")},
        {"data.runtime.nodejs.dependencies.vue", Field("3.0.0")},
        {"data.runtime.nodejs.dependencies.lodash", Field("4.17.21")},
        {"data.runtime.nodejs.name", Field("node-service")}
    })));
    sort_key_column->insert(Field(Int64(1)));

    const IColumn * columns[] = {json_column.get(), sort_key_column.get()};
    func->add(place, columns, 0, &arena);

    WriteBufferFromOwnString write_buf;
    func->serialize(place, write_buf, std::nullopt);

    ReadBufferFromString read_buf(write_buf.str());

    bool root_has_terminal = false;
    readBoolText(root_has_terminal, read_buf);
    EXPECT_FALSE(root_has_terminal);

    size_t root_children = 0;
    readVarUInt(root_children, read_buf);
    EXPECT_EQ(root_children, 1);

    String root_child_name;
    readStringBinary(root_child_name, read_buf);
    EXPECT_EQ(root_child_name, "data");

    bool data_has_terminal = false;
    readBoolText(data_has_terminal, read_buf);
    EXPECT_FALSE(data_has_terminal);

    size_t data_children = 0;
    readVarUInt(data_children, read_buf);
    EXPECT_EQ(data_children, 1);

    func->destroy(place);
}

TEST(AggregateFunctionMergedJSONPatch, StateMemoryConsumptionMetrics)
{
    tryRegisterAggregateFunctions();

    auto func = createMergedJSONPatchFunction(true);

    const auto repeated_updates_rows = makeRepeatedUpdatesDataset(256);
    const auto repeated_updates_keys = makeIncreasingSortKeys(repeated_updates_rows.size());
    const auto repeated_updates_metrics = measureStateSize(func, repeated_updates_rows, repeated_updates_keys);

    const auto wide_dependency_rows = makeWideDependencyDataset(256);
    const auto wide_dependency_keys = makeIncreasingSortKeys(wide_dependency_rows.size());
    const auto wide_dependency_metrics = measureStateSize(func, wide_dependency_rows, wide_dependency_keys);

    EXPECT_GT(repeated_updates_metrics.allocated_bytes, 0);
    EXPECT_GT(repeated_updates_metrics.serialized_bytes, 0);
    EXPECT_GT(wide_dependency_metrics.allocated_bytes, 0);
    EXPECT_GT(wide_dependency_metrics.serialized_bytes, 0);

    EXPECT_LT(repeated_updates_metrics.serialized_bytes, 1024UL);
    EXPECT_LT(repeated_updates_metrics.allocated_bytes, 32 * 1024UL);

    EXPECT_LT(wide_dependency_metrics.serialized_bytes, 32 * 1024UL);
    EXPECT_LT(wide_dependency_metrics.allocated_bytes, 128 * 1024UL);

    std::cerr
        << "mergedJSONPatch state metrics: repeated_updates allocated=" << repeated_updates_metrics.allocated_bytes
        << " serialized=" << repeated_updates_metrics.serialized_bytes
        << "; wide_dependencies allocated=" << wide_dependency_metrics.allocated_bytes
        << " serialized=" << wide_dependency_metrics.serialized_bytes
        << std::endl;
}




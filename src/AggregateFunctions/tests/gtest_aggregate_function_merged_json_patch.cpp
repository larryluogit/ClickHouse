#include <gtest/gtest.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Arena.h>
#include <Common/tests/gtest_global_register.h>
#include <base/defines.h>

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




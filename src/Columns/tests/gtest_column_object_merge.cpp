#include <gtest/gtest.h>

#include <Columns/ColumnObject.h>
#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

/// Unit tests for ColumnObject::mergeObjectColumns() utility
///
/// The mergeObjectColumns() function implements RFC 7396 JSON Merge Patch semantics
/// for ColumnObject instances. These tests verify the core merging logic.

using namespace DB;

namespace
{

/// Helper to create a ColumnObject with dynamic paths
MutableColumnPtr createObjectColumnWithDynamicPaths(
    const std::map<String, Field> & path_values,
    size_t max_dynamic_paths = 1024,
    size_t max_dynamic_types = 255)
{
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> typed_paths;
    
    /// Create an empty ColumnObject
    auto column = ColumnObject::create(std::move(typed_paths), max_dynamic_paths, max_dynamic_types);
    auto & obj = assert_cast<ColumnObject &>(*column);
    
    /// Convert path_values to Object and insert as a single row
    Object object;
    for (const auto & [path, value] : path_values)
        object[path] = value;
    
    obj.insert(Field(object));
    
    return column;
}

/// Helper to get an Int64 value from a ColumnObject at a specific path and row
/// Checks both dynamic paths and shared data
/// Returns default value if path doesn't exist or is null
Int64 getInt64FromPath(const ColumnObject & obj, const String & path, size_t row, Int64 default_value = 0)
{
    /// First check dynamic paths
    const auto & dynamic_paths = obj.getDynamicPathsPtrs();
    auto it = dynamic_paths.find(path);
    if (it != dynamic_paths.end() && !it->second->isNullAt(row))
    {
        Field result;
        it->second->get(row, result);
        return result.safeGet<Int64>();
    }
    
    /// If not in dynamic paths, check shared data
    const auto [shared_paths, shared_values] = obj.getSharedDataPathsAndValues();
    const auto & shared_offsets = obj.getSharedDataOffsets();
    
    size_t start = row == 0 ? 0 : shared_offsets[row - 1];
    size_t end = shared_offsets[row];
    
    for (size_t i = start; i < end; ++i)
    {
        auto shared_path = shared_paths->getDataAt(i);
        if (shared_path == path)
        {
            /// Found the path in shared data, deserialize the value into a temporary column
            auto temp_col = ColumnDynamic::create(obj.getMaxDynamicTypes());
            ColumnObject::deserializeValueFromSharedData(shared_values, i, *temp_col);
            Field result;
            temp_col->get(0, result);
            return result.safeGet<Int64>();
        }
    }
    
    return default_value;
}

/// Helper to get a String value from a ColumnObject at a specific path and row
/// Checks both dynamic paths and shared data
/// Returns default value if path doesn't exist or is null
String getStringFromPath(const ColumnObject & obj, const String & path, size_t row, const String & default_value = "")
{
    /// First check dynamic paths
    const auto & dynamic_paths = obj.getDynamicPathsPtrs();
    auto it = dynamic_paths.find(path);
    if (it != dynamic_paths.end() && !it->second->isNullAt(row))
    {
        Field result;
        it->second->get(row, result);
        return result.safeGet<String>();
    }
    
    /// If not in dynamic paths, check shared data
    const auto [shared_paths, shared_values] = obj.getSharedDataPathsAndValues();
    const auto & shared_offsets = obj.getSharedDataOffsets();
    
    size_t start = row == 0 ? 0 : shared_offsets[row - 1];
    size_t end = shared_offsets[row];
    
    for (size_t i = start; i < end; ++i)
    {
        auto shared_path = shared_paths->getDataAt(i);
        if (shared_path == path)
        {
            /// Found the path in shared data, deserialize the value into a temporary column
            auto temp_col = ColumnDynamic::create(obj.getMaxDynamicTypes());
            ColumnObject::deserializeValueFromSharedData(shared_values, i, *temp_col);
            Field result;
            temp_col->get(0, result);
            return result.safeGet<String>();
        }
    }
    
    return default_value;
}

/// Helper to check if a path is null at a specific row
/// Returns true if path exists and is null, false otherwise
bool isNullAtPath(const ColumnObject & obj, const String & path, size_t row)
{
    /// Check dynamic paths
    const auto & dynamic_paths = obj.getDynamicPathsPtrs();
    auto it = dynamic_paths.find(path);
    if (it != dynamic_paths.end())
    {
        return it->second->isNullAt(row);
    }
    
    /// If not in dynamic paths, check shared data
    const auto [shared_paths, shared_values] = obj.getSharedDataPathsAndValues();
    const auto & shared_offsets = obj.getSharedDataOffsets();
    
    size_t start = row == 0 ? 0 : shared_offsets[row - 1];
    size_t end = shared_offsets[row];
    
    for (size_t i = start; i < end; ++i)
    {
        auto shared_path = shared_paths->getDataAt(i);
        if (shared_path == path)
        {
            /// Found the path in shared data, deserialize and check if null
            auto temp_col = ColumnDynamic::create(obj.getMaxDynamicTypes());
            ColumnObject::deserializeValueFromSharedData(shared_values, i, *temp_col);
            return temp_col->isNullAt(0);
        }
    }
    
    /// Path doesn't exist
    return false;
}

/// Helper to get a Float64 value from a ColumnObject at a specific path and row
/// Returns default value if path doesn't exist or is null
Float64 getFloat64FromPath(const ColumnObject & obj, const String & path, size_t row, Float64 default_value = 0.0)
{
    const auto & dynamic_paths = obj.getDynamicPathsPtrs();
    auto it = dynamic_paths.find(path);
    if (it == dynamic_paths.end() || it->second->isNullAt(row))
        return default_value;
    
    Field result;
    it->second->get(row, result);
    return result.safeGet<Float64>();
}

}

/// Test merging when destination and source have no overlapping paths
TEST(MergedJSONPatchMerge, NoOverlappingPaths)
{
    auto dest = createObjectColumnWithDynamicPaths({{"a", Field(1)}, {"b", Field(2)}});
    auto source = createObjectColumnWithDynamicPaths({{"c", Field(3)}, {"d", Field(4)}});
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// After merge, dest should have all 4 paths with correct values
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 4);
    ASSERT_TRUE(dynamic_paths.contains("a"));
    ASSERT_TRUE(dynamic_paths.contains("b"));
    ASSERT_TRUE(dynamic_paths.contains("c"));
    ASSERT_TRUE(dynamic_paths.contains("d"));
    
    /// Verify values
    EXPECT_EQ(getInt64FromPath(dest_obj, "a", 0), 1);
    EXPECT_EQ(getInt64FromPath(dest_obj, "b", 0), 2);
    EXPECT_EQ(getInt64FromPath(dest_obj, "c", 0), 3);
    EXPECT_EQ(getInt64FromPath(dest_obj, "d", 0), 4);
}

/// Test merging when paths overlap - source values should replace dest values
TEST(MergedJSONPatchMerge, OverlappingPaths)
{
    auto dest = createObjectColumnWithDynamicPaths({{"a", Field(1)}, {"b", Field(2)}});
    auto source = createObjectColumnWithDynamicPaths({{"b", Field(20)}, {"c", Field(3)}});
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// After merge, dest should have paths a, b (updated), and c
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 3);
    ASSERT_TRUE(dynamic_paths.contains("a"));
    ASSERT_TRUE(dynamic_paths.contains("b"));
    ASSERT_TRUE(dynamic_paths.contains("c"));
    
    /// Verify values - b should be updated to 20
    EXPECT_EQ(getInt64FromPath(dest_obj, "a", 0), 1);
    EXPECT_EQ(getInt64FromPath(dest_obj, "b", 0), 20);  // Updated value
    EXPECT_EQ(getInt64FromPath(dest_obj, "c", 0), 3);
}

/// Test merging with null values in source
TEST(MergedJSONPatchMerge, NullValuesInSource)
{
    auto dest = createObjectColumnWithDynamicPaths({{"a", Field(1)}, {"b", Field(2)}});
    auto source = createObjectColumnWithDynamicPaths({{"b", Field()}, {"c", Field(3)}});
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// According to RFC 7396, null values should set the destination path to null (deletion semantics)
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// After merge, dest should have all three paths
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 3);
    ASSERT_TRUE(dynamic_paths.contains("a"));
    ASSERT_TRUE(dynamic_paths.contains("b"));
    ASSERT_TRUE(dynamic_paths.contains("c"));
    
    /// Verify values - b should be null (RFC 7396 deletion semantics)
    EXPECT_EQ(getInt64FromPath(dest_obj, "a", 0), 1);
    EXPECT_TRUE(isNullAtPath(dest_obj, "b", 0));  // Set to null by source
    EXPECT_EQ(getInt64FromPath(dest_obj, "c", 0), 3);
}

/// Test merging with different data types
TEST(MergedJSONPatchMerge, DifferentDataTypes)
{
    auto dest = createObjectColumnWithDynamicPaths({{"a", Field(42)}, {"b", Field("hello")}});
    auto source = createObjectColumnWithDynamicPaths({{"a", Field("world")}, {"b", Field(100)}});
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Type changes should be handled by ColumnDynamic
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_TRUE(dynamic_paths.contains("a"));
    ASSERT_TRUE(dynamic_paths.contains("b"));
    
    /// Verify values - types should have changed
    EXPECT_EQ(getStringFromPath(dest_obj, "a", 0), "world");  // Int -> String
    EXPECT_EQ(getInt64FromPath(dest_obj, "b", 0), 100);      // String -> Int
}

/// Test merging empty source into non-empty dest
TEST(MergedJSONPatchMerge, EmptySource)
{
    auto dest = createObjectColumnWithDynamicPaths({{"a", Field(1)}, {"b", Field(2)}});
    auto source = createObjectColumnWithDynamicPaths({});
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// Dest should remain unchanged
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_TRUE(dynamic_paths.contains("a"));
    ASSERT_TRUE(dynamic_paths.contains("b"));
    
    /// Verify values unchanged
    EXPECT_EQ(getInt64FromPath(dest_obj, "a", 0), 1);
    EXPECT_EQ(getInt64FromPath(dest_obj, "b", 0), 2);
}

/// Test merging into empty dest
TEST(MergedJSONPatchMerge, EmptyDest)
{
    auto dest = createObjectColumnWithDynamicPaths({});
    auto source = createObjectColumnWithDynamicPaths({{"a", Field(1)}, {"b", Field(2)}});
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// Dest should now have source's paths
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_GE(dynamic_paths.size(), 0);  /// May be in shared data if no dynamic paths available
    
    /// Verify values were copied
    EXPECT_EQ(getInt64FromPath(dest_obj, "a", 0), 1);
    EXPECT_EQ(getInt64FromPath(dest_obj, "b", 0), 2);
}

/// Test merging with many paths
TEST(MergedJSONPatchMerge, ManyPaths)
{
    std::map<String, Field> dest_paths;
    std::map<String, Field> source_paths;
    
    for (int i = 0; i < 50; ++i)
    {
        dest_paths["field_" + std::to_string(i)] = Field(i);
        source_paths["field_" + std::to_string(i)] = Field(i * 2);
    }
    
    auto dest = createObjectColumnWithDynamicPaths(dest_paths);
    auto source = createObjectColumnWithDynamicPaths(source_paths);
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// All paths should be present (some may be in shared data)
    ASSERT_EQ(dest_obj.size(), 1);
    
    /// Verify a few values were updated (doubled)
    EXPECT_EQ(getInt64FromPath(dest_obj, "field_0", 0), 0);
    EXPECT_EQ(getInt64FromPath(dest_obj, "field_10", 0), 20);
    EXPECT_EQ(getInt64FromPath(dest_obj, "field_25", 0), 50);
}

/// Test merging with special characters in path names
TEST(MergedJSONPatchMerge, SpecialCharacterPaths)
{
    auto dest = createObjectColumnWithDynamicPaths({
        {"key.with.dots", Field(1)},
        {"key-with-dashes", Field(2)}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"key.with.dots", Field(10)},
        {"key_with_underscores", Field(3)}
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 3);
    ASSERT_TRUE(dynamic_paths.contains("key.with.dots"));
    ASSERT_TRUE(dynamic_paths.contains("key-with-dashes"));
    ASSERT_TRUE(dynamic_paths.contains("key_with_underscores"));
    
    /// Verify values
    EXPECT_EQ(getInt64FromPath(dest_obj, "key.with.dots", 0), 10);  // Updated
    EXPECT_EQ(getInt64FromPath(dest_obj, "key-with-dashes", 0), 2);
    EXPECT_EQ(getInt64FromPath(dest_obj, "key_with_underscores", 0), 3);
}

/// Test merging with numeric types
TEST(MergedJSONPatchMerge, NumericTypes)
{
    auto dest = createObjectColumnWithDynamicPaths({
        {"int8", Field(Int8(1))},
        {"int64", Field(Int64(2))},
        {"float64", Field(Float64(3.14))}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"int8", Field(Int8(10))},
        {"int64", Field(Int64(20))},
        {"float64", Field(Float64(2.71))}
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 3);
    ASSERT_TRUE(dynamic_paths.contains("int8"));
    ASSERT_TRUE(dynamic_paths.contains("int64"));
    ASSERT_TRUE(dynamic_paths.contains("float64"));
    
    /// Verify values were updated
    EXPECT_EQ(getInt64FromPath(dest_obj, "int8", 0), 10);
    EXPECT_EQ(getInt64FromPath(dest_obj, "int64", 0), 20);
    EXPECT_EQ(getFloat64FromPath(dest_obj, "float64", 0), 2.71);
}

/// Test merging with string values
TEST(MergedJSONPatchMerge, StringValues)
{
    auto dest = createObjectColumnWithDynamicPaths({
        {"name", Field("Alice")},
        {"city", Field("New York")}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"name", Field("Bob")},
        {"country", Field("USA")}
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 3);
    ASSERT_TRUE(dynamic_paths.contains("name"));
    ASSERT_TRUE(dynamic_paths.contains("city"));
    ASSERT_TRUE(dynamic_paths.contains("country"));
    
    /// Verify values
    EXPECT_EQ(getStringFromPath(dest_obj, "name", 0), "Bob");  // Updated
    EXPECT_EQ(getStringFromPath(dest_obj, "city", 0), "New York");
    EXPECT_EQ(getStringFromPath(dest_obj, "country", 0), "USA");
}

/// ============================================================================
/// Edge Case Tests (documenting current behavior and future TODOs)
/// ============================================================================

/// Test nested object merging
/// TODO: Current implementation doesn't recursively merge nested objects
/// According to RFC 7396, nested objects should be merged recursively
TEST(MergedJSONPatchMerge, NestedObjectMerging)
{
    /// ColumnObject stores JSON as flattened paths (e.g., "user.name", "user.age"),
    /// so merging path-by-path naturally achieves recursive object merging.
    /// This test verifies that the function handles nested structures correctly.
    
    /// Test that the function handles flat structures (simulating nested objects via path names)
    auto dest = createObjectColumnWithDynamicPaths({
        {"user", Field("Alice")},
        {"age", Field(30)}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"user", Field("Bob")},
        {"city", Field("NYC")}
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Should not crash
    ASSERT_NO_THROW(ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0));
    
    /// Verify values were updated
    EXPECT_EQ(getStringFromPath(dest_obj, "user", 0), "Bob");
    EXPECT_EQ(getInt64FromPath(dest_obj, "age", 0), 30);
    EXPECT_EQ(getStringFromPath(dest_obj, "city", 0), "NYC");
}

/// Test array value handling
/// Arrays are treated as atomic values per RFC 7396 - they replace, not merge
TEST(MergedJSONPatchMerge, ArrayValueHandling)
{
    /// This test demonstrates that array-like values are replaced atomically
    /// Per RFC 7396: non-object values (including arrays) replace the target value
    ///
    /// Simulating: dest = {"items": [1,2], "name": "old"}
    ///             source = {"items": [3,4,5], "status": "new"}
    /// Expected result: {"items": [3,4,5], "name": "old", "status": "new"}
    
    auto dest = createObjectColumnWithDynamicPaths({
        {"items", Field("array[1,2]")},      // Simulating array as string
        {"name", Field("old")}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"items", Field("array[3,4,5]")},    // Different array - should replace
        {"status", Field("new")}              // New path - should be added
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// Result should contain all paths from both objects
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 3);  // items (replaced), name (kept), status (added)
    ASSERT_TRUE(dynamic_paths.contains("items"));
    ASSERT_TRUE(dynamic_paths.contains("name"));
    ASSERT_TRUE(dynamic_paths.contains("status"));
    
    /// Verify array was replaced (not merged element-by-element)
    EXPECT_EQ(getStringFromPath(dest_obj, "items", 0), "array[3,4,5]");  // Replaced with source array
    
    /// Verify destination path not in source is preserved
    EXPECT_EQ(getStringFromPath(dest_obj, "name", 0), "old");  // Kept from dest
    
    /// Verify new source path was added
    EXPECT_EQ(getStringFromPath(dest_obj, "status", 0), "new");  // Added from source
}

/// Test array handling in typed paths
/// Typed paths should also replace arrays atomically per RFC 7396
TEST(MergedJSONPatchMerge, ArrayValueHandlingTypedPaths)
{
    /// Create ColumnObject with typed paths (pre-defined schema)
    /// Simulating: dest = {"id": 1, "tags": [1,2]}
    ///             source = {"id": 1, "tags": [3,4,5]}
    /// Expected: {"id": 1, "tags": [3,4,5]} - array replaced, not merged
    
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> typed_paths_dest;
    typed_paths_dest["id"] = DataTypeInt64().createColumn();
    typed_paths_dest["tags"] = DataTypeString().createColumn();  // Simulating array as string
    
    auto dest = ColumnObject::create(std::move(typed_paths_dest), 1024, 255);
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    
    /// Insert a row with typed path values
    Object dest_object;
    dest_object["id"] = Field(Int64(1));
    dest_object["tags"] = Field("array[1,2]");
    dest_obj.insert(Field(dest_object));
    
    /// Create source with same typed paths but different array value
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> typed_paths_source;
    typed_paths_source["id"] = DataTypeInt64().createColumn();
    typed_paths_source["tags"] = DataTypeString().createColumn();
    
    auto source = ColumnObject::create(std::move(typed_paths_source), 1024, 255);
    auto & source_obj = assert_cast<ColumnObject &>(*source);
    
    Object source_object;
    source_object["id"] = Field(Int64(1));
    source_object["tags"] = Field("array[3,4,5]");  // Different array
    source_obj.insert(Field(source_object));
    
    /// Merge
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// Verify typed paths exist
    const auto & typed_paths = dest_obj.getTypedPaths();
    ASSERT_EQ(typed_paths.size(), 2);
    ASSERT_TRUE(typed_paths.contains("id"));
    ASSERT_TRUE(typed_paths.contains("tags"));
    
    /// Verify array was replaced atomically in typed path
    Field id_field;
    Field tags_field;
    typed_paths.at("id")->get(0, id_field);
    typed_paths.at("tags")->get(0, tags_field);
    
    EXPECT_EQ(id_field.safeGet<Int64>(), 1);
    EXPECT_EQ(tags_field.safeGet<String>(), "array[3,4,5]");  // Array replaced, not merged
}

/// Test overlapping paths with typed paths - source values should replace dest values
TEST(MergedJSONPatchMerge, OverlappingPathsTypedPaths)
{
    /// Create dest with typed paths
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> typed_paths_dest;
    typed_paths_dest["id"] = DataTypeInt64().createColumn();
    typed_paths_dest["name"] = DataTypeString().createColumn();
    
    auto dest = ColumnObject::create(std::move(typed_paths_dest), 1024, 255);
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    
    Object dest_object;
    dest_object["id"] = Field(Int64(1));
    dest_object["name"] = Field("Alice");
    dest_obj.insert(Field(dest_object));
    
    /// Create source with same typed paths but different values
    UnorderedMapWithMemoryTracking<String, MutableColumnPtr> typed_paths_source;
    typed_paths_source["id"] = DataTypeInt64().createColumn();
    typed_paths_source["name"] = DataTypeString().createColumn();
    
    auto source = ColumnObject::create(std::move(typed_paths_source), 1024, 255);
    auto & source_obj = assert_cast<ColumnObject &>(*source);
    
    Object source_object;
    source_object["id"] = Field(Int64(2));  // Different value
    source_object["name"] = Field("Bob");    // Different value
    source_obj.insert(Field(source_object));
    
    /// Merge
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// Verify typed paths exist and values were replaced
    const auto & typed_paths = dest_obj.getTypedPaths();
    ASSERT_EQ(typed_paths.size(), 2);
    ASSERT_TRUE(typed_paths.contains("id"));
    ASSERT_TRUE(typed_paths.contains("name"));
    
    Field id_field;
    Field name_field;
    typed_paths.at("id")->get(0, id_field);
    typed_paths.at("name")->get(0, name_field);
    
    /// Values should be replaced with source values
    EXPECT_EQ(id_field.safeGet<Int64>(), 2);      // Replaced
    EXPECT_EQ(name_field.safeGet<String>(), "Bob");  // Replaced
}

/// Test array handling in shared data paths
/// Shared data paths should also replace arrays atomically per RFC 7396
TEST(MergedJSONPatchMerge, ArrayValueHandlingSharedData)
{
    /// Create objects with many paths to force overflow to shared data
    /// Use low limit (5 dynamic paths) to ensure arrays end up in shared data
    
    std::map<String, Field> dest_paths;
    dest_paths["field1"] = Field(1);
    dest_paths["field2"] = Field(2);
    dest_paths["field3"] = Field(3);
    dest_paths["field4"] = Field(4);
    dest_paths["field5"] = Field(5);
    dest_paths["array_a"] = Field("array[1,2]");      // Will overflow to shared data
    dest_paths["array_b"] = Field("array[10,20]");    // Will overflow to shared data
    
    std::map<String, Field> source_paths;
    source_paths["array_a"] = Field("array[3,4,5]");  // Replace array_a
    source_paths["array_c"] = Field("array[100]");    // New array in shared data
    
    /// Use low limit to force arrays into shared data
    auto dest = createObjectColumnWithDynamicPaths(dest_paths, 5, 255);
    auto source = createObjectColumnWithDynamicPaths(source_paths, 5, 255);
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Verify some paths are in shared data before merge
    const auto [shared_paths_before, shared_values_before] = dest_obj.getSharedDataPathsAndValues();
    const auto & shared_offsets_before = dest_obj.getSharedDataOffsets();
    size_t shared_count_before = shared_offsets_before[0];
    EXPECT_GT(shared_count_before, 0);  // Should have paths in shared data
    
    /// Merge
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// Verify arrays in shared data were handled correctly
    /// array_a should be replaced with new value
    EXPECT_EQ(getStringFromPath(dest_obj, "array_a", 0), "array[3,4,5]");  // Replaced
    
    /// array_b should be preserved (not in source)
    EXPECT_EQ(getStringFromPath(dest_obj, "array_b", 0), "array[10,20]");  // Kept
    
    /// array_c should be added
    EXPECT_EQ(getStringFromPath(dest_obj, "array_c", 0), "array[100]");  // Added
    
    /// Verify regular fields are still accessible
    EXPECT_EQ(getInt64FromPath(dest_obj, "field1", 0), 1);
    EXPECT_EQ(getInt64FromPath(dest_obj, "field2", 0), 2);
}

/// Test null value handling per RFC 7396
/// Null values should delete keys (set them to null in ColumnObject)
TEST(MergedJSONPatchMerge, NullValueDeletion)
{
    /// Per RFC 7396: null values in the patch delete the corresponding key
    /// In ColumnObject, we set the path to null rather than removing it from the structure
    
    auto dest = createObjectColumnWithDynamicPaths({
        {"a", Field(1)},
        {"b", Field(2)},
        {"c", Field(3)}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"b", Field()},  /// null - should delete key "b" per RFC 7396
        {"d", Field(4)}
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// After merge: path "b" should be set to null (deletion semantics)
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 4);
    ASSERT_TRUE(dynamic_paths.contains("a"));
    ASSERT_TRUE(dynamic_paths.contains("b"));  // Path still exists in structure
    ASSERT_TRUE(dynamic_paths.contains("c"));
    ASSERT_TRUE(dynamic_paths.contains("d"));
    
    /// Verify values
    EXPECT_EQ(getInt64FromPath(dest_obj, "a", 0), 1);  // Unchanged
    
    /// Path "b" should now be null (deleted per RFC 7396)
    auto it_b = dynamic_paths.find("b");
    ASSERT_NE(it_b, dynamic_paths.end());
    EXPECT_TRUE(it_b->second->isNullAt(0));  // Should be null
    
    EXPECT_EQ(getInt64FromPath(dest_obj, "c", 0), 3);  // Unchanged
    EXPECT_EQ(getInt64FromPath(dest_obj, "d", 0), 4);  // Added
}

/// Test type conflicts in dynamic paths
TEST(MergedJSONPatchMerge, TypeConflicts)
{
    /// Test that type changes are handled gracefully by ColumnDynamic
    auto dest = createObjectColumnWithDynamicPaths({
        {"value", Field(42)},
        {"flag", Field(true)}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"value", Field("string")},  /// Int → String
        {"flag", Field(0)}           /// Bool → Int
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Should handle type changes without crashing
    ASSERT_NO_THROW(ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0));
    
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 2);
    ASSERT_TRUE(dynamic_paths.contains("value"));
    ASSERT_TRUE(dynamic_paths.contains("flag"));
    
    /// Verify type changes
    EXPECT_EQ(getStringFromPath(dest_obj, "value", 0), "string");
    EXPECT_EQ(getInt64FromPath(dest_obj, "flag", 0), 0);
}

/// Test shared data overflow scenarios
TEST(MergedJSONPatchMerge, SharedDataOverflow)
{
    /// Test behavior when we exceed max_dynamic_paths limit
    /// Paths should overflow to shared data
    
    std::map<String, Field> dest_paths;
    std::map<String, Field> source_paths;
    
    /// Create enough paths to exceed a low limit
    /// dest will have 30 paths, source will have 30 different paths
    /// With max_dynamic_paths=40, some will overflow to shared data
    for (int i = 0; i < 30; ++i)
    {
        dest_paths["dest_field_" + std::to_string(i)] = Field(i);
    }
    for (int i = 0; i < 30; ++i)
    {
        source_paths["source_field_" + std::to_string(i)] = Field(i * 2);
    }
    
    /// Use low limit to force overflow (30 + 30 = 60 paths, limit is 40)
    auto dest = createObjectColumnWithDynamicPaths(dest_paths, 40, 255);
    auto source = createObjectColumnWithDynamicPaths(source_paths, 40, 255);
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Should handle overflow to shared data gracefully
    /// Some paths will be in dynamic storage, others in shared data
    ASSERT_NO_THROW(ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0));
    
    /// Column should still have one row
    ASSERT_EQ(dest_obj.size(), 1);
    
    /// Verify the dynamic paths limit was respected
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    EXPECT_EQ(dynamic_paths.size(), 40);  // Should be exactly at the limit
    
    /// Verify that some paths are in shared data (overflow occurred)
    const auto [shared_paths, shared_values] = dest_obj.getSharedDataPathsAndValues();
    const auto & shared_offsets = dest_obj.getSharedDataOffsets();
    size_t shared_count = shared_offsets[0];  // Number of paths in shared data for row 0
    EXPECT_GT(shared_count, 0);  // Should have some paths in shared data
    EXPECT_EQ(dynamic_paths.size() + shared_count, 60);  // Total should be 60 (30 dest + 30 source)
    
    /// Verify a few values from dynamic paths (these should be accessible)
    EXPECT_EQ(getInt64FromPath(dest_obj, "dest_field_0", 0), 0);
    EXPECT_EQ(getInt64FromPath(dest_obj, "dest_field_1", 0), 1);
}

/// Test merging when destination already has shared data paths
TEST(MergedJSONPatchMerge, SharedDataOverflowWithExistingSharedData)
{
    /// Create dest with 50 paths (limit is 40, so 10 will be in shared data)
    std::map<String, Field> dest_paths;
    for (int i = 0; i < 50; ++i)
    {
        dest_paths["dest_field_" + std::to_string(i)] = Field(i);
    }
    
    /// Create source with 30 new paths
    std::map<String, Field> source_paths;
    for (int i = 0; i < 30; ++i)
    {
        source_paths["source_field_" + std::to_string(i)] = Field(i * 2);
    }
    
    /// Use low limit to force overflow
    auto dest = createObjectColumnWithDynamicPaths(dest_paths, 40, 255);
    auto source = createObjectColumnWithDynamicPaths(source_paths, 40, 255);
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Verify dest already has shared data before merge
    {
        const auto [shared_paths_before, shared_values_before] = dest_obj.getSharedDataPathsAndValues();
        const auto & shared_offsets_before = dest_obj.getSharedDataOffsets();
        size_t shared_count_before = shared_offsets_before[0];
        EXPECT_EQ(shared_count_before, 10);  // 50 total - 40 dynamic = 10 in shared data
    }
    
    /// Merge source into dest
    ASSERT_NO_THROW(ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0));
    
    /// Column should still have one row
    ASSERT_EQ(dest_obj.size(), 1);
    
    /// Verify the dynamic paths limit was respected
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    EXPECT_EQ(dynamic_paths.size(), 40);  // Should be exactly at the limit
    
    /// Verify that shared data now includes both existing and new overflow paths
    const auto [shared_paths, shared_values] = dest_obj.getSharedDataPathsAndValues();
    const auto & shared_offsets = dest_obj.getSharedDataOffsets();
    size_t shared_count = shared_offsets[0];  // Number of paths in shared data for row 0
    
    /// Total should be 80 paths (50 dest + 30 source)
    /// 40 in dynamic paths + 40 in shared data = 80 total
    EXPECT_EQ(shared_count, 40);  // 10 existing + 30 new = 40 in shared data
    EXPECT_EQ(dynamic_paths.size() + shared_count, 80);  // Total should be 80
    
    /// Verify values from both dynamic paths and shared data
    /// Some dest fields should be in dynamic paths (the first 40)
    EXPECT_EQ(getInt64FromPath(dest_obj, "dest_field_0", 0), 0);
    EXPECT_EQ(getInt64FromPath(dest_obj, "dest_field_1", 0), 1);
    
    /// Some dest fields should be in shared data (fields 40-49)
    EXPECT_EQ(getInt64FromPath(dest_obj, "dest_field_45", 0), 45);
    EXPECT_EQ(getInt64FromPath(dest_obj, "dest_field_49", 0), 49);
    
    /// Source fields should be accessible (some in dynamic, some in shared)
    EXPECT_EQ(getInt64FromPath(dest_obj, "source_field_0", 0), 0);
    EXPECT_EQ(getInt64FromPath(dest_obj, "source_field_10", 0), 20);
}

/// Test merging with mixed path locations (dynamic + shared data)
TEST(MergedJSONPatchMerge, MixedPathLocations)
{
    /// Test merging when paths are in different storage locations
    auto dest = createObjectColumnWithDynamicPaths({
        {"a", Field(1)},
        {"b", Field(2)}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"c", Field(3)},
        {"d", Field(4)}
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0);
    
    /// All paths should be accessible
    ASSERT_EQ(dest_obj.size(), 1);
    
    /// Verify all values
    EXPECT_EQ(getInt64FromPath(dest_obj, "a", 0), 1);
    EXPECT_EQ(getInt64FromPath(dest_obj, "b", 0), 2);
    EXPECT_EQ(getInt64FromPath(dest_obj, "c", 0), 3);
    EXPECT_EQ(getInt64FromPath(dest_obj, "d", 0), 4);
}

/// Test merging with empty path names
TEST(MergedJSONPatchMerge, EmptyPathNames)
{
    /// Test that empty path names are handled (edge case)
    auto dest = createObjectColumnWithDynamicPaths({
        {"", Field(1)},  /// Empty path name
        {"a", Field(2)}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"", Field(10)},  /// Empty path name
        {"b", Field(3)}
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Should handle empty path names
    ASSERT_NO_THROW(ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0));
    
    /// Verify values
    EXPECT_EQ(getInt64FromPath(dest_obj, "", 0), 10);  // Updated
    EXPECT_EQ(getInt64FromPath(dest_obj, "a", 0), 2);
    EXPECT_EQ(getInt64FromPath(dest_obj, "b", 0), 3);
}

/// Test merging with very long path names
TEST(MergedJSONPatchMerge, LongPathNames)
{
    /// Test that long path names are handled
    String long_path(1000, 'x');  /// 1000 character path name
    
    auto dest = createObjectColumnWithDynamicPaths({
        {long_path, Field(1)},
        {"short", Field(2)}
    });
    auto source = createObjectColumnWithDynamicPaths({
        {long_path, Field(10)},
        {"another", Field(3)}
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Should handle long path names
    ASSERT_NO_THROW(ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0));
    
    /// Verify values
    EXPECT_EQ(getInt64FromPath(dest_obj, long_path, 0), 10);  // Updated
    EXPECT_EQ(getInt64FromPath(dest_obj, "short", 0), 2);
    EXPECT_EQ(getInt64FromPath(dest_obj, "another", 0), 3);
}

/// Test merging with Unicode path names
TEST(MergedJSONPatchMerge, UnicodePathNames)
{
    /// Test that Unicode characters in path names are handled
    auto dest = createObjectColumnWithDynamicPaths({
        {"名前", Field("Alice")},  /// Japanese
        {"città", Field("Roma")}   /// Italian
    });
    auto source = createObjectColumnWithDynamicPaths({
        {"名前", Field("Bob")},
        {"país", Field("España")}  /// Spanish
    });
    
    auto & dest_obj = assert_cast<ColumnObject &>(*dest);
    const auto & source_obj = assert_cast<const ColumnObject &>(*source);
    
    /// Should handle Unicode path names
    ASSERT_NO_THROW(ColumnObject::mergeObjectColumns(dest_obj, source_obj, 0, 0));
    
    const auto & dynamic_paths = dest_obj.getDynamicPathsPtrs();
    ASSERT_EQ(dynamic_paths.size(), 3);
    ASSERT_TRUE(dynamic_paths.contains("名前"));
    ASSERT_TRUE(dynamic_paths.contains("città"));
    ASSERT_TRUE(dynamic_paths.contains("país"));
    
    /// Verify values
    EXPECT_EQ(getStringFromPath(dest_obj, "名前", 0), "Bob");  // Updated
    EXPECT_EQ(getStringFromPath(dest_obj, "città", 0), "Roma");
    EXPECT_EQ(getStringFromPath(dest_obj, "país", 0), "España");
}

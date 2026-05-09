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
            ReadBufferFromString buf(std::string_view(data.data, data.size));
            return decodeField(buf);
        }

    };

    struct Entry
    {
        StringSlice key;
        EncodedField value;
        SortKey sort_key;
    };

    Arena path_arena;
    Arena value_arena;
    std::vector<Entry> entries;
    mutable bool is_compacted = true;

    AggregateFunctionMergedJSONPatchData() = default;

    static StringSlice copyToArena(Arena & arena, std::string_view data)
    {
        char * dst = arena.alloc(data.size());
        memcpy(dst, data.data(), data.size());
        return StringSlice(dst, data.size());
    }

    EncodedField encodeFieldToArena(Field value)
    {
        WriteBufferFromOwnString buf;
        encodeField(value, buf);
        return EncodedField{copyToArena(value_arena, buf.str())};
    }

    static constexpr size_t COMPACTION_THRESHOLD = 4096;

    void ensureCompacted() const
    {
        if (!is_compacted)
            const_cast<AggregateFunctionMergedJSONPatchData *>(this)->compactEntries();
    }

    static std::string_view getKeyView(const Entry & entry)
    {
        return std::string_view(entry.key.data, entry.key.size);
    }

    static std::string_view getValueView(const Entry & entry)
    {
        return std::string_view(entry.value.data.data, entry.value.data.size);
    }

    static bool isPathPrefix(std::string_view prefix, std::string_view path)
    {
        return path.size() > prefix.size()
            && path.starts_with(prefix)
            && path[prefix.size()] == '.';
    }

    void compactEntries()
    {
        if (is_compacted)
            return;

        std::sort(entries.begin(), entries.end(), [](const Entry & lhs, const Entry & rhs)
        {
            return getKeyView(lhs) < getKeyView(rhs);
        });

        std::vector<Entry> deduplicated;
        deduplicated.reserve(entries.size());

        for (const auto & entry : entries)
        {
            if (!deduplicated.empty() && getKeyView(deduplicated.back()) == getKeyView(entry))
            {
                if (deduplicated.back().sort_key <= entry.sort_key)
                {
                    deduplicated.back().key = entry.key;
                    deduplicated.back().value = entry.value;
                    deduplicated.back().sort_key = entry.sort_key;
                }
            }
            else
            {
                deduplicated.push_back(entry);
            }
        }

        std::vector<Entry> compacted;
        compacted.reserve(deduplicated.size());

        for (const auto & entry : deduplicated)
        {
            std::string_view key = getKeyView(entry);
            bool skip = false;

            for (auto it = compacted.begin(); it != compacted.end();)
            {
                std::string_view existing_key = getKeyView(*it);
                if (isPathPrefix(key, existing_key) || isPathPrefix(existing_key, key))
                {
                    if (it->sort_key <= entry.sort_key)
                    {
                        it = compacted.erase(it);
                    }
                    else
                    {
                        skip = true;
                        break;
                    }
                }
                else
                {
                    ++it;
                }
            }

            if (!skip)
                compacted.push_back(entry);
        }

        entries = std::move(compacted);
        is_compacted = true;
    }

    void add(const IColumn & json_column, size_t row_num, Arena * arena)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        
        /// Use a serialized representation of the object as sort key for deterministic ordering
        const char * begin = nullptr;
        auto serialized = object_column.serializeValueIntoArena(row_num, *arena, begin, nullptr);
        SortKey sort_key = SortKey(Field(String(serialized)));
        
        /// Extract all key-value pairs from the JSON object
        addKeyValuePairs(object_column, row_num, sort_key);
    }

    void addWithKey(const IColumn & json_column, const IColumn & key_column, size_t row_num, Arena *)
    {
        const auto & object_column = assert_cast<const ColumnObject &>(json_column);
        
        /// Get sort key
        SortKey sort_key = SortKey(key_column[row_num]);
        
        /// Extract all key-value pairs from the JSON object
        addKeyValuePairs(object_column, row_num, sort_key);
    }

    void appendPathValue(std::string_view key, Field value, const SortKey & sort_key)
    {
        if (!is_compacted)
        {
            for (auto & entry : entries)
            {
                std::string_view existing_key = getKeyView(entry);

                if (existing_key == key)
                {
                    if (entry.sort_key <= sort_key)
                    {
                        entry.key = copyToArena(path_arena, key);
                        entry.value = encodeFieldToArena(std::move(value));
                        entry.sort_key = sort_key;
                    }
                    return;
                }

                if (isPathPrefix(key, existing_key))
                {
                    if (entry.sort_key <= sort_key)
                    {
                        entry.key = copyToArena(path_arena, key);
                        entry.value = encodeFieldToArena(std::move(value));
                        entry.sort_key = sort_key;
                    }
                    return;
                }

                if (isPathPrefix(existing_key, key))
                {
                    if (entry.sort_key > sort_key)
                        return;
                }
            }
        }

        entries.push_back(Entry{
            .key = copyToArena(path_arena, key),
            .value = encodeFieldToArena(std::move(value)),
            .sort_key = sort_key
        });
        is_compacted = false;

        if (entries.size() >= COMPACTION_THRESHOLD)
            compactEntries();
    }

    void addKeyValuePairs(const ColumnObject & object_column, size_t row_num, const SortKey & sort_key)
    {
        /// Use SortedPathsIterator to iterate over all paths (typed + dynamic + shared data)
        ColumnObject::SortedPathsIterator it(object_column, row_num);
        while (!it.end())
        {
            auto path_info = it.getCurrentPathInfo();

            /// Get the value for this path
            Field value;
            path_info.column->get(path_info.row, value);
            normalizeMixedJSONArray(value);

            appendPathValue(path_info.path, std::move(value), sort_key);

            it.next();
        }
    }

    void merge(const AggregateFunctionMergedJSONPatchData & other, Arena *)
    {
        /// Merge key-value pairs from other state, keeping only the latest for each key.
        /// This implements last-write-wins semantics (RFC 7396 core behavior): for each path,
        /// the value with the largest sorting_key wins, which is equivalent to merging objects
        /// in sorted order. Note: RFC 7396 null deletion is not supported (see limitation above).
        ensureCompacted();
        other.ensureCompacted();

        std::vector<Entry> merged;
        merged.reserve(entries.size() + other.entries.size());

        size_t lhs = 0;
        size_t rhs = 0;

        while (lhs < entries.size() || rhs < other.entries.size())
        {
            const Entry * candidate = nullptr;

            if (rhs >= other.entries.size())
            {
                candidate = &entries[lhs++];
            }
            else if (lhs >= entries.size())
            {
                candidate = &other.entries[rhs++];
            }
            else
            {
                std::string_view lhs_key = getKeyView(entries[lhs]);
                std::string_view rhs_key = getKeyView(other.entries[rhs]);

                if (lhs_key < rhs_key)
                    candidate = &entries[lhs++];
                else
                    candidate = &other.entries[rhs++];
            }

            std::string_view candidate_key = getKeyView(*candidate);
            bool skip = false;

            for (auto it = merged.begin(); it != merged.end();)
            {
                std::string_view existing_key = getKeyView(*it);
                if (existing_key == candidate_key)
                {
                    if (it->sort_key <= candidate->sort_key)
                    {
                        *it = Entry{
                            .key = copyToArena(path_arena, candidate_key),
                            .value = EncodedField{copyToArena(value_arena, getValueView(*candidate))},
                            .sort_key = candidate->sort_key
                        };
                    }
                    skip = true;
                    break;
                }

                if (isPathPrefix(candidate_key, existing_key) || isPathPrefix(existing_key, candidate_key))
                {
                    if (it->sort_key <= candidate->sort_key)
                    {
                        it = merged.erase(it);
                    }
                    else
                    {
                        skip = true;
                        break;
                    }
                }
                else
                {
                    ++it;
                }
            }

            if (!skip)
            {
                merged.push_back(Entry{
                    .key = copyToArena(path_arena, candidate_key),
                    .value = EncodedField{copyToArena(value_arena, getValueView(*candidate))},
                    .sort_key = candidate->sort_key
                });
            }
        }

        entries = std::move(merged);
        is_compacted = true;
    }

    void serialize(WriteBuffer & buf) const
    {
        ensureCompacted();

        writeVarUInt(entries.size(), buf);

        for (const auto & entry : entries)
        {
            writeStringBinary(getKeyView(entry), buf);
            writeStringBinary(getValueView(entry), buf);
            encodeField(entry.sort_key.toField(), buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        entries.reserve(entries.size() + size);

        for (size_t i = 0; i < size; ++i)
        {
            String key;
            readStringBinary(key, buf);

            String value_data;
            readStringBinary(value_data, buf);

            SortKey sort_key = SortKey(decodeField(buf));

            ReadBufferFromString value_buf(value_data);
            appendPathValue(key, decodeField(value_buf), sort_key);
        }
    }

    void insertResultInto(IColumn & to, const DataTypePtr &) const
    {
        auto & result_column = assert_cast<ColumnObject &>(to);

        ensureCompacted();

        if (entries.empty())
        {
            result_column.insertDefault();
            return;
        }

        Object result_object;
        for (const auto & entry : entries)
        {
            Field value = entry.value.get();
            if (value.isNull())
                continue;

            result_object[String(entry.key.data, entry.key.size)] = std::move(value);
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

#include "apps/transport/http.h"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

namespace glamdb::NTransport {
TEST(Transport, HealthStateFixturesKeepReadinessSeparateFromDurability)
{
    std::ifstream input(
        std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() / "fixtures/http/health-states.json");
    const auto fixtures = TJson::parse(input);
    const std::vector<StoreHealth> states{
        {StoreState::Recovering},
        {StoreState::Ready, true, true},
        {StoreState::RecoveryRequired},
        {StoreState::StorageFailed, true, false, true},
        {StoreState::Unavailable},
        {StoreState::Stopping},
        {StoreState::Closed},
    };
    for (size_t index = 0; index < states.size(); ++index) {
        const auto response = ReadinessResponse(states[index]);
        EXPECT_EQ(response.Status, fixtures[index]["status"]);
        EXPECT_EQ(TJson::parse(response.Body), fixtures[index]["response"]);
    }
}

TEST(Transport, RejectsAmbiguousJsonAndFraming)
{
    EXPECT_THROW(ParseJson(R"({"operation_id":"a","operation_id":"b"})"), TFailure);
    EXPECT_THROW(ParseJson(std::string(162, '[') + "0" + std::string(162, ']')), TFailure);
    for (const auto* header : {"Content-Length: 1\r\nContent-Length: 1\r\n",
             "Transfer-Encoding: chunked\r\n",
             " Content-Length: 1\r\n",
             "Content-Length : 1\r\n",
             "Content-Length: +1\r\n",
             "Content-Length: 1,1\r\n"})
    {
        EXPECT_THROW(ParseHeaders(std::string("POST /collections HTTP/1.1\r\nHost: local\r\n") + header + "\r\n"),
            TFailure);
    }
}
TEST(Transport, PreservesExactScalarsAndRejectsInvalidValues)
{
    const SchemaSpec schema{
        {{"key", FieldType::String, false}, {"n", FieldType::Int64, true}, {"t", FieldType::TimestampUsUtc, true}}};
    const auto batch = ParseRows(schema,
        ParseJson(
            R"([{"key":"x","n":"-9223372036854775808","t":"0001-01-01T00:00:00Z"},{"key":"x","n":"9223372036854775807","t":"9999-12-31T23:59:59.999999Z"}])"));
    const auto rows = EncodeRows(*batch);
    EXPECT_EQ(rows[0]["n"], "-9223372036854775808");
    EXPECT_EQ(rows[0]["t"], "0001-01-01T00:00:00.000000Z");
    EXPECT_EQ(rows[1]["n"], "9223372036854775807");
    EXPECT_EQ(rows[1]["t"], "9999-12-31T23:59:59.999999Z");
    for (const auto* timestamp : {"0000-01-01T00:00:00Z",
             "2026-02-30T00:00:00Z",
             "2026-01-01T24:00:00Z",
             "2026-01-01T00:00:60Z",
             "2026-01-01T00:00:00.0000001Z",
             "0001-01-01T00:00:00+01:00"})
    {
        EXPECT_THROW(ParseRows(schema, TJson::array({{{"key", "x"}, {"t", timestamp}}})), TFailure);
    }
    EXPECT_THROW(ParseInt64(TJson(true)), TFailure);
    EXPECT_THROW(ParseInt64(TJson(9007199254740993LL)), TFailure);
    EXPECT_THROW(ParseInt64(TJson("9223372036854775808")), TFailure);
}
TEST(Transport, RejectsUnknownFieldsAndUnsupportedQueries)
{
    const SchemaSpec schema{{{"key", FieldType::String, false}}};
    EXPECT_FALSE(ParseScan("id", schema, ParseJson(R"({"projection":null})")).projection.has_value());
    EXPECT_THROW(ParseRows(schema, ParseJson(R"([{"key":"x","extra":1}])")), TFailure);
    EXPECT_THROW(ParseScan("id", schema, ParseJson(R"({"projection":[]})")), TFailure);
    EXPECT_THROW(ParseScan("id", schema, ParseJson(R"({"predicate":{"op":"or","args":[]}})")), TFailure);
    EXPECT_THROW(ParseScan("id", schema, ParseJson(R"({"limit":true})")), TFailure);
    EXPECT_THROW(ParseScan("id", schema, ParseJson(R"({"limit":-1})")), TFailure);
    EXPECT_THROW(ParseTarget("/collections?name=a&name=b"), TFailure);
    EXPECT_THROW(ParseTarget("/%0a"), TFailure);
}
TEST(Transport, QueryValuesPreserveAcceptedNamesWithoutRelaxingPaths)
{
    EXPECT_EQ(ParseTarget("/collections:resolve?name=a%0Ab%09c%7Fd%2B%26%3D").second.at("name"),
        "a\nb\tc\x7F"
        "d+&=");
    EXPECT_THROW(ParseTarget("/collections%0A?name=a"), TFailure);
    EXPECT_THROW(ParseTarget("/collections?na%0Ame=a"), TFailure);
    EXPECT_THROW(ParseTarget("/collections?name=a%00b"), TFailure);
}
TEST(Transport, ErrorDiagnosticsStayBoundedAndUtf8Valid)
{
    const auto error = EncodeError({"INVALID_ARGUMENT", std::string(MaxRequestBytes, 'x') + "я"});
    EXPECT_LT(error.dump().size(), 16384U);
    EXPECT_EQ(error.at("error").at("code"), "INVALID_ARGUMENT");
    EXPECT_EQ(error.at("error").at("retry_hint"), "FixInput");
    EXPECT_NO_THROW(ParseJson(EncodeError({"IO_ERROR", std::string(2047, 'x') + "я"}).dump()));
}
TEST(Transport, ScanAcceptsCoreBoundsWithoutUnsignedOverflow)
{
    const SchemaSpec schema{{{"key", FieldType::String, false}}};
    EXPECT_EQ(ParseScan("id", schema, ParseJson(R"({"batch_rows":65536})")).batch_rows, 65536);
    EXPECT_THROW(ParseScan("id", schema, ParseJson(R"({"batch_rows":65537})")), TFailure);
    EXPECT_EQ(ParseScan("id", schema, ParseJson(R"({"limit":100001})")).limit, 100001);
    EXPECT_EQ(ParseScan("id", schema, ParseJson(R"({"limit":9223372036854775807})")).limit,
        std::numeric_limits<int64_t>::max());
    EXPECT_THROW(ParseScan("id", schema, ParseJson(R"({"limit":9223372036854775808})")), TFailure);
    EXPECT_THROW(ParseScan("id", schema, ParseJson(R"({"batch_rows":18446744073709551615})")), TFailure);
}
TEST(Transport, ParsesLargeObjectArrayWithoutChangingScalars)
{
    constexpr std::string_view Row =
        R"({"key":"same","n":9007199254740993,"f":1.25,"b":true,"nil":null,"s":"\u0430","a":[0,false]})";
    std::string input = "[";
    for (size_t i = 0; i < 10000; ++i) {
        if (i != 0) {
            input += ',';
        }
        input += Row;
    }
    input += ']';
    const auto rows = ParseJson(input);
    ASSERT_EQ(rows.size(), 10000U);
    EXPECT_EQ(rows.front(), rows.back());
    EXPECT_EQ(rows.back()["n"].get<int64_t>(), 9007199254740993LL);
    EXPECT_EQ(rows.back()["s"], "а");
    EXPECT_EQ(rows.back()["a"], TJson::array({0, false}));
    EXPECT_TRUE(rows.back()["nil"].is_null());
}
TEST(Transport, RejectsLateDecodedDuplicateKeyInLargeArray)
{
    std::string input = "[";
    for (size_t i = 0; i < 10000; ++i) {
        input += R"({"key":"same"},)";
    }
    input += R"({"outer":[{"key":1,"\u006bey":2}] }])";
    EXPECT_THROW(ParseJson(input), TFailure);
    EXPECT_EQ(ParseJson(R"([{"key":1},{"key":2,"nested":{"key":3}}])").size(), 2U);
}
TEST(Transport, PreservesJsonDepthAndEventBoundaries)
{
    EXPECT_NO_THROW(ParseJson(std::string(160, '[') + "0" + std::string(160, ']')));
    EXPECT_THROW(ParseJson(std::string(161, '[') + "0" + std::string(161, ']')), TFailure);
    EXPECT_NO_THROW(ParseJson(std::string(161, '[') + std::string(161, ']')));
    EXPECT_THROW(ParseJson(std::string(162, '[') + std::string(162, ']')), TFailure);
    // Container start/end each count once, exactly like the original callback.
    std::string input = "[0";
    for (size_t i = 1; i < MaxJsonEvents - 2; ++i) {
        input += ",0";
    }
    EXPECT_EQ(ParseJson(input + "]").size(), MaxJsonEvents - 2);
    EXPECT_THROW(ParseJson(input + ",0]"), TFailure);
}
TEST(Transport, AcceptsAdvertisedWideAppendAndTypedValues)
{
    TJson row = TJson::object();
    SchemaSpec schema;
    for (int i = 0; i < 49; ++i) {
        const auto name = "field" + std::to_string(i);
        schema.fields.push_back({name, FieldType::Int64, false});
        row[name] = "1";
    }
    auto rows = TJson::array();
    for (int i = 0; i < 10000; ++i) {
        rows.push_back(row);
    }
    auto parsed = ParseJson(TJson({{"rows", rows}}).dump());
    const auto batch = ParseRows(schema, parsed["rows"]);
    EXPECT_EQ(batch->num_rows(), 10000);
    EXPECT_EQ(batch->num_columns(), 49);
    const SchemaSpec types{
        {{"flag", FieldType::Bool, false}, {"f", FieldType::Float64, true}, {"ts", FieldType::TimestampUsUtc, false}}};
    const auto typed = ParseRows(types, ParseJson(R"([{"flag":true,"f":1.25,"ts":"2026-01-01T01:30:00.123+01:30"}])"));
    EXPECT_EQ(EncodeRows(*typed)[0]["ts"], "2026-01-01T00:00:00.123000Z");
    EXPECT_EQ(EncodeRows(*typed)[0]["flag"], true);
    EXPECT_THROW(ParseRows(types, ParseJson(R"([{"flag":1,"ts":"2026-01-01T00:00:00Z"}])")), TFailure);
}
TEST(Transport, ErrorPolicyPreservesOperationalRecovery)
{
    EXPECT_EQ(ErrorStatus("STALE_BASE"), 409);
    EXPECT_EQ(ErrorStatus("OPERATION_IN_PROGRESS"), 503);
    EXPECT_EQ(ErrorStatus("CAPACITY_EXCEEDED"), 507);
    EXPECT_EQ(ErrorStatus("UNSUPPORTED_TABLE_FEATURE"), 501);
    EXPECT_EQ(ErrorStatus("DEADLINE_EXCEEDED"), 504);
    for (const auto* code : {"DATABASE_BUSY", "RECOVERY_REQUIRED", "SERVER_BUSY"}) {
        try {
            Require(false, "blocked", code);
            FAIL();
        } catch (const TFailure& error) {
            EXPECT_EQ(error.Detail.retry_hint, RetryHint::SameIdAfterReconcile);
        }
    }
    EXPECT_EQ(
        ParseJson(
            ErrorResponse(503, "INTERNAL", "unknown", RetryHint::SameIdAfterReconcile).Body)["error"]["retry_hint"],
        "SameIdAfterReconcile");
    EXPECT_THROW(ParseTarget("/databases/id%2Fcollections/healthz"), TFailure);
}
TEST(Transport, ExhaustedCapacityIs507AndAdmissionIs429)
{
    using enum RetryHint;
    EXPECT_EQ(ErrorStatus(ErrorDetail{"RESOURCE_EXHAUSTED", "storage preflight", OperatorAction}), 507);
    EXPECT_EQ(ErrorStatus(ErrorDetail{"CAPACITY_EXCEEDED", "catalog full", OperatorAction}), 507);
    for (const auto hint : {SameIdAfterReconcile, NewOperationId, FixInput}) {
        EXPECT_EQ(ErrorStatus(ErrorDetail{"RESOURCE_EXHAUSTED", "pending budget is full", hint}), 429);
    }
    EXPECT_EQ(ErrorStatus("RESOURCE_EXHAUSTED"), 429);
    try {
        Require(false, "scan session limit", "RESOURCE_EXHAUSTED");
        FAIL();
    } catch (const TFailure& error) {
        EXPECT_EQ(ErrorStatus(error.Detail), 429);
    }
    EXPECT_EQ(ErrorStatus(ErrorDetail{"OPERATION_IN_PROGRESS", "busy", OperatorAction}), 503);
}
TEST(Transport, RejectsInvalidUtf8AndMalformedJsonTokens)
{
    for (const auto& input : {std::string("[\"") + char(0xFF) + "\"]",
             std::string(R"({"x":"\ud800"})"),
             std::string("[1,]"),
             std::string("{} {}"),
             std::string("[1e9999]")})
    {
        EXPECT_ANY_THROW(ParseJson(input));
    }
}

TEST(Transport, ParsesCompoundPredicatesAndEnforcesLimitsAndErrorCodes)
{
    const SchemaSpec schema{{
        {"key", FieldType::String, false},
        {"n", FieldType::Int64, true},
        {"f", FieldType::Float64, true},
        {"b", FieldType::Bool, true},
        {"t", FieldType::TimestampUsUtc, true},
        {"s", FieldType::String, true},
    }};

    const auto compound = ParseScan("id", schema, ParseJson(R"({
            "predicate": {
                "op": "and",
                "args": [
                    {"op": "true"},
                    {"op": "eq", "field": "key", "value": "k1"},
                    {"op": "ne", "field": "n", "value": "10"},
                    {"op": "lt", "field": "f", "value": -0.0},
                    {"op": "le", "field": "b", "value": true},
                    {"op": "gt", "field": "t", "value": "2026-01-01T00:00:00Z"},
                    {"op": "ge", "field": "s", "value": null},
                    {"op": "in", "field": "n", "values": []},
                    {"op": "in", "field": "n", "values": ["1", null, "2"]},
                    {"op": "is_null", "field": "s"},
                    {"op": "is_not_null", "field": "n"},
                    {"op": "or", "args": [
                        {"op": "not", "arg": {"op": "eq", "field": "n", "value": null}},
                        {"op": "true"}
                    ]}
                ]
            }
        })"));
    EXPECT_EQ(compound.predicate.kind(), Predicate::Kind::And);
    ASSERT_EQ(compound.predicate.children().size(), 12U);
    EXPECT_EQ(compound.predicate.children()[3].value(), Literal{0.0});
    EXPECT_EQ(compound.predicate.children()[6].kind(), Predicate::Kind::Compare);
    EXPECT_FALSE(compound.predicate.children()[6].optional_value().has_value());

    // Depth 64 compound predicate parses cleanly through both ParseJson (<= 160) and ParseScan (<= 64).
    std::string depth64 = R"({"predicate":)";
    for (int d = 0; d < 64; ++d) {
        depth64 += R"({"op":"and","args":[)";
    }
    depth64 += R"({"op":"true"})";
    for (int d = 0; d < 64; ++d) {
        depth64 += "]}";
    }
    depth64 += "}";
    EXPECT_NO_THROW(ParseScan("id", schema, ParseJson(depth64)));

    auto expectFailure = [&](std::string_view body, int expectedStatus, std::string_view expectedCode) {
        try {
            (void)ParseScan("id", schema, ParseJson(body));
            FAIL() << "Expected TFailure for: " << body;
        } catch (const TFailure& error) {
            EXPECT_EQ(ErrorStatus(error.Detail), expectedStatus) << body;
            EXPECT_EQ(error.Detail.code, expectedCode) << body;
            EXPECT_EQ(error.Detail.retry_hint,
                expectedCode == "INVALID_ARGUMENT" ? RetryHint::FixInput : RetryHint::OperatorAction)
                << body;
        } catch (const std::exception& ex) {
            FAIL() << "Expected TFailure, got std::exception: " << ex.what() << " for " << body;
        }
    };

    // Malformed JSON shapes/types and invalid arguments -> 400 INVALID_ARGUMENT.
    for (const std::string_view badInput : {
             std::string_view{R"({"predicate":123})"},
             std::string_view{R"({"predicate":[]})"},
             std::string_view{R"({"predicate":{}})"},
             std::string_view{R"({"predicate":{"op":123}})"},
             std::string_view{R"({"predicate":{"op":"true","extra":1}})"},
             std::string_view{R"({"predicate":{"op":"and"}})"},
             std::string_view{R"({"predicate":{"op":"and","args":123}})"},
             std::string_view{R"({"predicate":{"op":"and","args":[]}})"},
             std::string_view{R"({"predicate":{"op":"or","args":[]}})"},
             std::string_view{R"({"predicate":{"op":"and","args":[null]}})"},
             std::string_view{R"({"predicate":{"op":"and","args":[123]}})"},
             std::string_view{R"({"predicate":{"op":"and","args":[{"op":"true"}],"extra":1}})"},
             std::string_view{R"({"predicate":{"op":"not"}})"},
             std::string_view{R"({"predicate":{"op":"not","arg":null}})"},
             std::string_view{R"({"predicate":{"op":"not","arg":123}})"},
             std::string_view{R"({"predicate":{"op":"not","arg":{"op":"true"},"extra":1}})"},
             std::string_view{R"({"predicate":{"op":"is_null"}})"},
             std::string_view{R"({"predicate":{"op":"is_null","field":123}})"},
             std::string_view{R"({"predicate":{"op":"is_null","field":"missing"}})"},
             std::string_view{R"({"predicate":{"op":"is_null","field":"n","value":null}})"},
             std::string_view{R"({"predicate":{"op":"in","field":"n"}})"},
             std::string_view{R"({"predicate":{"op":"in","field":"n","values":123}})"},
             std::string_view{R"({"predicate":{"op":"in","field":"n","values":["1"],"extra":1}})"},
             std::string_view{R"({"predicate":{"op":"in","field":"n","values":[true]}})"},
             std::string_view{R"({"predicate":{"op":"eq","field":"n"}})"},
             std::string_view{R"({"predicate":{"op":"eq","field":"n","value":"1","extra":1}})"},
             std::string_view{R"({"predicate":{"op":"eq","field":"n","value":true}})"},
         })
    {
        expectFailure(badInput, 400, "INVALID_ARGUMENT");
    }

    // String byte caps: key > 1024 bytes and non-key string > 1 MiB -> 400 INVALID_ARGUMENT.
    expectFailure(TJson({{"predicate", {{"op", "eq"}, {"field", "key"}, {"value", std::string(1025, 'k')}}}}).dump(),
        400,
        "INVALID_ARGUMENT");
    expectFailure(TJson({{"predicate", {{"op", "in"}, {"field", "key"}, {"values", {std::string(1025, 'k')}}}}}).dump(),
        400,
        "INVALID_ARGUMENT");
    expectFailure(
        TJson({{"predicate", {{"op", "eq"}, {"field", "s"}, {"value", std::string(1024 * 1024 + 1, 's')}}}}).dump(),
        400,
        "INVALID_ARGUMENT");

    // Depth 65 -> 400 INVALID_ARGUMENT even if leaf at depth 65 has an unknown op ("xor").
    std::string depth65 = R"({"predicate":)";
    for (int d = 0; d < 65; ++d) {
        depth65 += R"({"op":"not","arg":)";
    }
    depth65 += R"({"op":"xor"})";
    for (int d = 0; d < 65; ++d) {
        depth65 += "}";
    }
    depth65 += "}";
    expectFailure(depth65, 400, "INVALID_ARGUMENT");

    // Nodes 1025 -> 400 INVALID_ARGUMENT.
    TJson args1024 = TJson::array();
    for (int i = 0; i < 1024; ++i) {
        args1024.push_back({{"op", "true"}});
    }
    expectFailure(TJson({{"predicate", {{"op", "and"}, {"args", args1024}}}}).dump(), 400, "INVALID_ARGUMENT");

    // Literals 4097 -> 400 INVALID_ARGUMENT.
    TJson vals4097 = TJson::array();
    for (int i = 0; i < 4097; ++i) {
        vals4097.push_back("1");
    }
    expectFailure(TJson({{"predicate", {{"op", "in"}, {"field", "n"}, {"values", vals4097}}}}).dump(),
        400,
        "INVALID_ARGUMENT");

    // Unknown op at depth <= 64 and empty projection -> 501 UNSUPPORTED_OPERATION.
    expectFailure(R"({"predicate":{"op":"xor","args":[{"op":"true"}]}})", 501, "UNSUPPORTED_OPERATION");
    expectFailure(R"({"predicate":{"op":"and","args":[{"op":"like","field":"key","value":"a%"}]}})",
        501,
        "UNSUPPORTED_OPERATION");
    expectFailure(R"({"projection":[]})", 501, "UNSUPPORTED_OPERATION");
}
} // namespace glamdb::NTransport

#include "apps/server/server.h"
#include "apps/transport/outcome_json.h"
#include "core/engine/engine_factory.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <iostream>

int main(int argc, char** argv)
try {
    if (argc != 2) {
        return 2;
    }
    const std::filesystem::path root(argv[1]);
    auto backend = std::make_shared<glamdb::test::FakeStorage>(root);
    auto engine = glamdb::NTransport::Unwrap(
        glamdb::detail::EngineFactory::Create({glamdb::test::store_id, root.string()}, backend));
    const auto created = engine->CreateCollection(
        {glamdb::test::collection_id, "events", {{{"key", glamdb::FieldType::String, false}}}});
    if (created.disposition != glamdb::CommitDisposition::Committed) {
        return 3;
    }
    backend->on_pointer = [] {
        std::cout << "publication\n" << std::flush;
        char release = 0;
        std::cin.get(release);
    };
    {
        glamdb::NServer::TRoutes routes(engine, {}, "test-token");
        glamdb::NServer::RunServer(routes, "127.0.0.1", 0, std::chrono::seconds(1));
        const glamdb::NTransport::TRequest scan{"POST",
            std::string("/collections/") + glamdb::test::collection_id + "/scan/open",
            {{"authorization", "Bearer test-token"}, {"content-type", "application/json"}},
            "{}"};
        const auto response = routes.Handle(scan);
        if (response.Status != 503 || glamdb::NTransport::ParseJson(response.Body)["error"]["code"] != "CANCELLED") {
            std::cerr << "scan admission remained open after shutdown: " << response.Body << '\n';
            return 7;
        }
    }
    if (!engine->Shutdown(std::chrono::seconds(10))) {
        return 4;
    }
    engine.reset();
    backend->on_pointer = {};
    engine = glamdb::NTransport::Unwrap(
        glamdb::detail::EngineFactory::Open({root.string(), glamdb::test::store_id}, backend));
    const auto outcome =
        glamdb::NTransport::Unwrap(engine->FindAppendOutcome(glamdb::test::collection_id, glamdb::test::operation_id));
    if (!outcome) {
        return 5;
    }
    std::cout << glamdb::NTransport::Unwrap(glamdb::json_adapter::ToJson(*outcome)).body << '\n';
    return outcome->disposition == glamdb::CommitDisposition::Committed ? 0 : 6;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}

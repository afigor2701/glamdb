#include "apps/server/databases.h"
#include "apps/server/server.h"
#include "tests/support/journal_fault_vfs.h"

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <thread>

// Test-only control channel on stdin; HTTP and every store operation use the
// production managed server and storage backend. No fault route is installed.
int main(int argc, char** argv)
try {
    if (argc != 2 && argc != 3) {
        return 2;
    }
    glamdb::test::JournalFaultVfs core, registry;
    auto* coreVfs = sqlite3_vfs_find(core.Name().c_str());
    if (sqlite3_vfs_register(coreVfs, 1) != SQLITE_OK) {
        return 3;
    }
    auto databases = std::make_shared<glamdb::NServer::TDatabases>(argv[1], registry.Name());
    glamdb::NServer::TRoutes routes(databases, {}, "fault-test-token");
    // RunServer owns cout's formatting state. Control replies use synchronized
    // stdio, and reading commands must not implicitly flush cout on this thread.
    std::cin.tie(nullptr);
    std::atomic<bool> drained{false};
    std::thread controls([&] {
        const auto reply = [](const char* text) {
            std::fputs(text, stdout);
            std::fflush(stdout);
        };
        std::vector<int> descriptors;
        rlimit original{};
        std::string line;
        while (std::getline(std::cin, line)) {
            std::istringstream command(line);
            std::string target;
            int commit = 0, after = 0;
            command >> target;
            if (target == "server-state") {
                reply(drained.load() ? "drained\n" : "running\n");
                continue;
            }
            if (target == "owner" || target == "shutdown") {
                std::string id;
                command >> id;
                if (target == "owner") {
                    const auto root = std::filesystem::path(argv[1]) / "databases" / id;
                    const auto opened = glamdb::OpenExisting({root.string(), id});
                    reply(opened ? "opened\n" : (opened.error().code + '\n').c_str());
                } else {
                    databases->Use(id, [&](const std::shared_ptr<glamdb::Engine>& engine) {
                        const auto closed = engine->Shutdown(std::chrono::milliseconds(1));
                        reply(closed ? "closed\n" : (closed.error().code + '\n').c_str());
                        return glamdb::NTransport::TResponse{};
                    });
                }
                continue;
            }
            command >> commit >> after;
            if (target == "exhaust-fds") {
                getrlimit(RLIMIT_NOFILE, &original);
                rlimit limit = original;
                limit.rlim_cur = std::min<rlim_t>(original.rlim_cur, 96);
                setrlimit(RLIMIT_NOFILE, &limit);
                for (;;) {
                    const int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
                    if (fd < 0) {
                        break;
                    }
                    descriptors.push_back(fd);
                }
                reply("armed\n");
                continue;
            }
            if (target == "release-fds") {
                for (const auto fd : descriptors) {
                    close(fd);
                }
                descriptors.clear();
                setrlimit(RLIMIT_NOFILE, &original);
                reply("armed\n");
                continue;
            }
            if (target == "pause") {
                core.PauseBeforeDelete(commit);
                reply("armed\n");
                continue;
            }
            if (target == "wait") {
                reply(core.WaitUntilPaused(std::chrono::seconds(10)) ? "paused\n" : "timeout\n");
                continue;
            }
            if (target == "release") {
                core.Release();
                reply("armed\n");
                continue;
            }
            auto& vfs = target == "registry" ? registry : core;
            if (commit == 0) {
                vfs.Disarm();
            } else {
                vfs.Arm(commit, after != 0);
            }
            reply("armed\n");
        }
        core.Release();
    });
    glamdb::NServer::RunServer(routes,
        "127.0.0.1",
        0,
        std::chrono::seconds(10),
        argc == 3 ? static_cast<size_t>(std::stoul(argv[2])) : 96);
    drained = true;
    controls.join();
    databases->Shutdown();
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}

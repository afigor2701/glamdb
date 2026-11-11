#pragma once

#include "apps/transport/http.h"

#include <chrono>
#include <memory>

namespace glamdb::NServer {
// Bounded, expiring fixed-view cursors. Tokens are scoped to database, collection
// and representation; only the last consumed page can be replayed.
class TScans
{
public:
    // Internal service policy; production routes use these bounded defaults.
    explicit TScans(std::chrono::milliseconds idle = std::chrono::seconds(60),
        std::chrono::milliseconds lifetime = std::chrono::minutes(5));
    ~TScans();
    NTransport::TResponse Open(const std::shared_ptr<Engine>& engine,
        const std::string& database,
        const std::string& collection,
        const NTransport::TJson& body,
        bool arrow,
        bool openOnly);
    NTransport::TResponse
    Cancel(const std::string& database, const std::string& collection, const NTransport::TJson& body);
    // Permanently close admission and mark sessions cancelled without waiting for reads.
    void Stop();
    void Clear(const std::string& database);
    void Clear();

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};
} // namespace glamdb::NServer

#include "apps/server/server.h"

#include <asio.hpp>

#include <atomic>
#include <iostream>
#include <set>

namespace glamdb::NServer {
using namespace NTransport;
namespace {
using TTcp = asio::ip::tcp;
class TSession;
struct TState
{
    explicit TState(TRoutes& routes, std::chrono::milliseconds timeout)
        : Routes(routes)
        , Timeout(timeout)
    { }
    asio::io_context IO;
    asio::thread_pool Workers{4};
    asio::thread_pool Control{2};
    asio::thread_pool Lifecycle{2};
    TRoutes& Routes;
    std::set<std::shared_ptr<TSession>> Sessions;
    std::map<std::string, size_t> DatabaseCalls;
    std::map<std::string, size_t> HeaderPeers;
    std::chrono::milliseconds Timeout;
    size_t Headers = 0, Bodies = 0, DataCalls = 0, ControlCalls = 0, LifecycleCalls = 0;
};

class TSession : public std::enable_shared_from_this<TSession>
{
public:
    TSession(TTcp::socket socket, TState& state)
        : Socket_(std::move(socket))
        , Timer_(state.IO)
        , State_(state)
    { }
    template <class F>
    void Guard(F&& action) noexcept
    {
        try {
            action();
        } catch (const TFailure& error) {
            try {
                Write(ErrorResponse(ErrorStatus(error.Detail),
                    error.Detail.code,
                    error.Detail.message,
                    error.Detail.retry_hint));
            } catch (...) {
                Finish();
            }
        } catch (...) {
            Finish();
        }
    }
    void Start()
    {
        Peer_ = Socket_.remote_endpoint().address().to_string();
        if (State_.Headers >= 64 || State_.HeaderPeers[Peer_] >= 32) {
            Busy();
            return;
        }
        ReadingHeaders_ = true;
        ++State_.Headers;
        ++State_.HeaderPeers[Peer_];
        Deadline(std::min(State_.Timeout, std::chrono::milliseconds(1000)), true);
        ReadHeaders();
    }
    void StopIdle()
    {
        if (!Processing_) {
            Finish();
        }
    }

private:
    void ReleaseHeaders()
    {
        if (ReadingHeaders_) {
            ReadingHeaders_ = false;
            --State_.Headers;
            const auto found = State_.HeaderPeers.find(Peer_);
            if (found != State_.HeaderPeers.end() && --found->second == 0) {
                State_.HeaderPeers.erase(found);
            }
        }
    }
    void ReadHeaders()
    {
        Socket_.async_read_some(asio::buffer(ReadBuffer_),
            [self = shared_from_this()](const std::error_code& error, size_t size) {
                self->Guard([&] {
                    if (self->Processing_) {
                        return;
                    }
                    if (error || self->Closed_) {
                        self->Finish();
                        return;
                    }
                    self->Input_.append(self->ReadBuffer_.data(), size);
                    const auto end = self->Input_.find("\r\n\r\n");
                    const auto headerSize = end == std::string::npos ? self->Input_.size() : end + 4;
                    Require(headerSize <= 16384, "HTTP headers exceed 16 KiB", "HEADERS_TOO_LARGE");
                    for (size_t i = self->Checked_; i < headerSize; ++i) {
                        Require(self->Input_[i] != '\n' || (i > 0 && self->Input_[i - 1] == '\r'),
                            "bare LF in HTTP headers");
                        Require(i == 0 || self->Input_[i - 1] != '\r' || self->Input_[i] == '\n',
                            "bare CR in HTTP headers");
                    }
                    self->Checked_ = headerSize;
                    if (end != std::string::npos) {
                        self->Headers(headerSize);
                    } else {
                        self->ReadHeaders();
                    }
                });
            });
    }
    void CancelDeadline()
    {
        ++Generation_;
        Timer_.cancel();
    }
    void Deadline(std::chrono::milliseconds timeout, bool reading = false)
    {
        const auto generation = ++Generation_;
        Timer_.expires_after(timeout);
        Timer_.async_wait([self = shared_from_this(), generation, reading](const std::error_code& error) {
            self->Guard([&] {
                if (error || self->Closed_ || generation != self->Generation_) {
                    return;
                }
                if (reading) {
                    self->Write(ErrorResponse(408, "REQUEST_TIMEOUT", "request read deadline exceeded"));
                } else {
                    self->Finish();
                }
            });
        });
    }
    void Finish() noexcept
    {
        if (Closed_) {
            return;
        }
        Closed_ = true;
        ++Generation_;
        std::error_code ignored;
        try {
            Timer_.cancel();
        } catch (...) {
            Socket_.cancel(ignored);
        }
        Socket_.close(ignored);
        ReleaseHeaders();
        if (BodyAdmitted_) {
            --State_.Bodies;
            BodyAdmitted_ = false;
        }
        ReleaseCall();
        State_.Sessions.erase(shared_from_this());
    }
    void Busy()
    {
        auto response =
            ErrorResponse(503, "SERVER_BUSY", "server admission limit reached; retry with original operation ID");
        response.Headers["Retry-After"] = "1";
        Write(std::move(response));
    }
    void Headers(size_t size)
    {
        if (Closed_ || Processing_) {
            return;
        }
        Request_ = ParseHeaders(std::string_view(Input_).substr(0, size));
        if (!State_.Routes.IsAuthorized(Request_)) {
            Write(ErrorResponse(401, "UNAUTHORIZED", "bearer token required"));
            return;
        }
        ReleaseHeaders();
        Input_.erase(0, size);
        Require(Input_.size() <= Request_.ContentLength, "pipelining is unsupported");
        if (Request_.ContentLength != 0) {
            if (State_.Bodies >= 16) {
                Busy();
                return;
            }
            ++State_.Bodies;
            BodyAdmitted_ = true;
        }
        Deadline(State_.Timeout, true);
        if (Request_.Headers.contains("expect")) {
            // A stalled interim write must close, not start an overlapping 408
            // write. The body read budget begins after Continue is delivered.
            Deadline(std::chrono::seconds(5));
            static constexpr std::string_view Continue = "HTTP/1.1 100 Continue\r\n\r\n";
            asio::async_write(Socket_,
                asio::buffer(Continue),
                [self = shared_from_this()](const std::error_code& error, size_t) {
                    self->Guard([&] {
                        if (error || self->Closed_) {
                            self->Finish();
                        } else {
                            self->Deadline(self->State_.Timeout, true);
                            self->ReadBody();
                        }
                    });
                });
        } else {
            ReadBody();
        }
    }
    void ReadBody()
    {
        if (Closed_ || Processing_) {
            return;
        }
        const auto remaining = Request_.ContentLength - Input_.size();
        if (remaining == 0) {
            Dispatch();
            return;
        }
        asio::async_read(Socket_,
            asio::dynamic_buffer(Input_, MaxRequestBytes),
            asio::transfer_exactly(remaining),
            [self = shared_from_this()](const std::error_code& error, size_t) {
                self->Guard([&] {
                    if (error) {
                        self->Finish();
                    } else {
                        self->Dispatch();
                    }
                });
            });
    }
    void ReleaseCall() noexcept
    {
        if (!Calls_) {
            return;
        }
        --*Calls_;
        Calls_ = nullptr;
        if (!Database_.empty()) {
            const auto found = State_.DatabaseCalls.find(Database_);
            if (found != State_.DatabaseCalls.end() && --found->second == 0) {
                State_.DatabaseCalls.erase(found);
            }
        }
    }
    // A worker cannot touch socket state. If allocating its completion fails,
    // the I/O thread observes this flag and closes only the affected session.
    void WatchWorker()
    {
        const auto generation = ++Generation_;
        Timer_.expires_after(std::chrono::milliseconds(100));
        Timer_.async_wait([self = shared_from_this(), generation](const std::error_code& error) {
            self->Guard([&] {
                if (error || self->Closed_ || generation != self->Generation_) {
                    return;
                }
                if (self->WorkerFailed_.load()) {
                    self->Finish();
                } else {
                    self->WatchWorker();
                }
            });
        });
    }
    void Dispatch()
    {
        if (Closed_ || Processing_) {
            return;
        }
        CancelDeadline();
        Request_.Body = std::move(Input_);
        const auto [path, query] = ParseTarget(Request_.Target);
        (void)query;
        const auto split = path.starts_with("/databases/") ? path.find('/', 11) : std::string::npos;
        const auto suffix = split == std::string::npos ? path : path.substr(split);
        // Health observation is bounded and does no I/O. Keep it available even
        // when operation lookups occupy every control worker behind a writer.
        if (Request_.Method == "GET" && (path == "/healthz" || suffix == "/capabilities" || suffix == "/readyz")) {
            Write(State_.Routes.Handle(Request_));
            return;
        }
        const bool lifecycle = Request_.Method == "POST" &&
            (path == "/databases" || suffix == "/open" || suffix == "/close" || suffix == "/reconcile" ||
                suffix == "/discard");
        const bool control = !lifecycle &&
            (path == "/databases" || (path.starts_with("/databases/") && split == std::string::npos) ||
                !suffix.starts_with("/collections") || suffix.find("/operations/") != std::string::npos ||
                suffix.ends_with("/scan/cancel"));
        auto& pool = lifecycle ? State_.Lifecycle : control ? State_.Control : State_.Workers;
        auto& count = lifecycle ? State_.LifecycleCalls : control ? State_.ControlCalls : State_.DataCalls;
        if (count >= (lifecycle ? 2U : control ? 8U : 4U)) {
            Busy();
            return;
        }
        if (!lifecycle && !control) {
            const auto database = split == std::string::npos ? std::string("legacy") : path.substr(11, split - 11);
            if (State_.DatabaseCalls[database] >= 2) {
                Busy();
                return;
            }
            Database_ = database;
            ++State_.DatabaseCalls[Database_];
        }
        ++count;
        Calls_ = &count;
        Processing_ = true;
        WatchWorker();
        asio::post(pool, [self = shared_from_this(), guard = asio::make_work_guard(State_.IO)]() mutable {
            try {
                auto response = self->State_.Routes.Handle(self->Request_);
                asio::post(self->State_.IO, [self, response = std::move(response), guard = std::move(guard)]() mutable {
                    self->Guard([&] {
                        self->ReleaseCall();
                        self->Write(std::move(response));
                    });
                });
            } catch (...) {
                self->WorkerFailed_.store(true);
            }
        });
    }
    void Write(TResponse response)
    {
        if (Closed_ || Writing_) {
            return;
        }
        ReleaseHeaders();
        Processing_ = true;
        Writing_ = true;
        Header_ = SerializeResponseHead(response);
        Output_ = std::move(response.Body);
        Deadline(std::chrono::seconds(5));
        const std::array buffers{asio::buffer(Header_), asio::buffer(Output_)};
        asio::async_write(Socket_, buffers, [self = shared_from_this()](const std::error_code&, size_t) {
            self->Finish();
        });
    }
    TTcp::socket Socket_;
    asio::steady_timer Timer_;
    TState& State_;
    TRequest Request_;
    std::array<char, 4096> ReadBuffer_{};
    std::string Input_, Header_, Output_, Peer_, Database_;
    size_t Checked_ = 0, Generation_ = 0;
    size_t* Calls_ = nullptr;
    std::atomic<bool> WorkerFailed_{false};
    bool ReadingHeaders_ = false, BodyAdmitted_ = false, Processing_ = false, Writing_ = false, Closed_ = false;
};
} // namespace

void RunServer(TRoutes& routes,
    std::string_view address,
    uint16_t port,
    std::chrono::milliseconds timeout,
    size_t maxConnections)
{
    Require(maxConnections > 0 && maxConnections <= 96, "connection ceiling must be 1..96");
    TState state(routes, timeout);
    TTcp::acceptor acceptor(state.IO, TTcp::endpoint(asio::ip::make_address(address), port));
    asio::steady_timer retry(state.IO);
    asio::signal_set signals(state.IO, SIGINT, SIGTERM);
    signals.async_wait([&](const std::error_code& error, int) {
        if (error) {
            return;
        }
        std::error_code ignored;
        acceptor.close(ignored);
        retry.cancel();
        routes.CancelScans();
        // StopIdle can erase itself; advancing before it avoids allocating a copy.
        for (auto it = state.Sessions.begin(); it != state.Sessions.end();) {
            (*it++)->StopIdle();
        }
    });
    std::function<void()> accept;
    bool accepting = false, waiting = false;
    const auto backoff = [&] {
        retry.expires_after(std::chrono::milliseconds(100));
        retry.async_wait([&](const std::error_code& error) {
            waiting = false;
            if (!error && acceptor.is_open()) {
                accept();
            }
        });
        waiting = true;
    };
    accept = [&] {
        if (!acceptor.is_open() || accepting || waiting) {
            return;
        }
        // Leave excess connections in the kernel backlog until a response slot
        // is available. Every accepted socket can then receive an HTTP response.
        if (state.Sessions.size() >= maxConnections) {
            backoff();
            return;
        }
        try {
            accepting = true;
            acceptor.async_accept([&](const std::error_code& error, TTcp::socket socket) {
                accepting = false;
                if (!acceptor.is_open()) {
                    return;
                }
                if (error) {
                    backoff();
                    return;
                }
                try {
                    auto session = std::make_shared<TSession>(std::move(socket), state);
                    state.Sessions.insert(session);
                    session->Guard([&] {
                        session->Start();
                    });
                } catch (...) {
                    std::error_code ignored;
                    socket.close(ignored);
                    backoff();
                    return;
                }
                accept();
            });
        } catch (...) {
            accepting = false;
            backoff();
        }
    };
    accept();
    std::cout << TJson({{"address", address}, {"port", acceptor.local_endpoint().port()}, {"backend", "local"}}).dump()
              << '\n'
              << std::flush;
    // Individual callbacks contain exceptions. An allocation failure while
    // scheduling accept backoff must not permanently abandon the listener.
    for (;;) {
        try {
            state.IO.run();
            break;
        } catch (...) {
            if (acceptor.is_open()) {
                accept();
            }
        }
    }
    state.Workers.join();
    state.Control.join();
    state.Lifecycle.join();
}
} // namespace glamdb::NServer

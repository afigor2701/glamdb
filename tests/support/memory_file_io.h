#pragma once

// In-memory iceberg::FileIO for planner/cursor tests: no filesystem access.

#include <iceberg/file_io.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>

namespace glamdb::test {

class TMemoryInputStream final : public iceberg::SeekableInputStream
{
public:
    explicit TMemoryInputStream(std::string bytes)
        : Bytes_(std::move(bytes))
    { }

    iceberg::Result<int64_t> Position() const override
    {
        return Position_;
    }

    iceberg::Status Seek(int64_t position) override
    {
        if (position < 0 || position > static_cast<int64_t>(Bytes_.size())) {
            return iceberg::InvalidArgument("invalid seek position");
        }
        Position_ = position;
        return {};
    }

    iceberg::Result<int64_t> Read(std::span<std::byte> out) override
    {
        const auto available = static_cast<int64_t>(Bytes_.size()) - Position_;
        const auto count = std::min<int64_t>(available, static_cast<int64_t>(out.size()));
        if (count > 0) {
            std::memcpy(out.data(), Bytes_.data() + Position_, static_cast<size_t>(count));
            Position_ += count;
        }
        return count;
    }

    iceberg::Status ReadFully(int64_t position, std::span<std::byte> out) override
    {
        if (position < 0 || position + static_cast<int64_t>(out.size()) > static_cast<int64_t>(Bytes_.size())) {
            return iceberg::Invalid("short read in memory stream");
        }
        std::memcpy(out.data(), Bytes_.data() + position, out.size());
        return {};
    }

    iceberg::Status Close() override
    {
        return {};
    }

private:
    std::string Bytes_;
    int64_t Position_ = 0;
};

class TMemoryOutputStream final : public iceberg::PositionOutputStream
{
public:
    TMemoryOutputStream(std::map<std::string, std::string>* files, std::string location)
        : Files_(files)
        , Location_(std::move(location))
    { }

    iceberg::Result<int64_t> Position() const override
    {
        return static_cast<int64_t>(Buffer_.size());
    }

    iceberg::Status Write(std::span<const std::byte> data) override
    {
        Buffer_.append(reinterpret_cast<const char*>(data.data()), data.size());
        return {};
    }

    iceberg::Status Flush() override
    {
        return {};
    }

    iceberg::Status Close() override
    {
        (*Files_)[Location_] = Buffer_;
        return {};
    }

private:
    std::map<std::string, std::string>* Files_;
    std::string Location_;
    std::string Buffer_;
};

class TMemoryInputFile final : public iceberg::InputFile
{
public:
    TMemoryInputFile(std::string location, std::string bytes)
        : Location_(std::move(location))
        , Bytes_(std::move(bytes))
    { }

    std::string_view location() const override
    {
        return Location_;
    }

    iceberg::Result<int64_t> Size() const override
    {
        return static_cast<int64_t>(Bytes_.size());
    }

    iceberg::Result<std::unique_ptr<iceberg::SeekableInputStream>> Open() override
    {
        return std::make_unique<TMemoryInputStream>(Bytes_);
    }

private:
    std::string Location_;
    std::string Bytes_;
};

class TMemoryOutputFile final : public iceberg::OutputFile
{
public:
    TMemoryOutputFile(std::map<std::string, std::string>* files, std::string location)
        : Files_(files)
        , Location_(std::move(location))
    { }

    std::string_view location() const override
    {
        return Location_;
    }

    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> Create() override
    {
        if (Files_->contains(Location_)) {
            return iceberg::AlreadyExists("memory file already exists");
        }
        return std::make_unique<TMemoryOutputStream>(Files_, Location_);
    }

    iceberg::Result<std::unique_ptr<iceberg::PositionOutputStream>> CreateOrOverwrite() override
    {
        return std::make_unique<TMemoryOutputStream>(Files_, Location_);
    }

private:
    std::map<std::string, std::string>* Files_;
    std::string Location_;
};

class TMemoryFileIO final : public iceberg::FileIO
{
public:
    std::map<std::string, std::string> Files;
    std::function<void(const std::string&)> OnRead;

    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location) override
    {
        if (OnRead) {
            OnRead(location);
        }
        const auto it = Files.find(location);
        if (it == Files.end()) {
            return iceberg::NotFound("memory file not found: {}", location);
        }
        return std::make_unique<TMemoryInputFile>(std::move(location), it->second);
    }

    iceberg::Result<std::unique_ptr<iceberg::InputFile>> NewInputFile(std::string location, size_t) override
    {
        return NewInputFile(std::move(location));
    }

    iceberg::Result<std::unique_ptr<iceberg::OutputFile>> NewOutputFile(std::string location) override
    {
        return std::make_unique<TMemoryOutputFile>(&Files, std::move(location));
    }

    iceberg::Result<std::string> ReadFile(const std::string& location, std::optional<size_t> length) override
    {
        if (OnRead) {
            OnRead(location);
        }
        const auto it = Files.find(location);
        if (it == Files.end()) {
            return iceberg::NotFound("memory file not found: {}", location);
        }
        if (length && *length < it->second.size()) {
            return it->second.substr(0, *length);
        }
        return it->second;
    }

    iceberg::Status WriteFile(const std::string& location, std::string_view content) override
    {
        Files[location] = std::string(content);
        return {};
    }

    iceberg::Status DeleteFile(const std::string& location) override
    {
        Files.erase(location);
        return {};
    }
};

} // namespace glamdb::test

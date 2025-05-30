#include "LogDB.h"
#include <iostream>
#include <sstream>
#include <msgpack.hpp>
#include <memory>
#include "ChronobreakFilter.h"
#include "Key.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

LogDB::LogDB(const std::string& path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compaction_filter_factory = std::make_shared<ChronobreakFilterFactory>();

    rocksdb::Status status = rocksdb::DB::Open(options, path, &db_);
    if (!status.ok()) {
        std::cerr << "Failed to open RocksDB: " << status.ToString() << std::endl;
        db_ = nullptr;
    } else {
        std::cout << "[INIT] RocksDB opened at path: " << path << std::endl;
    }
}

LogDB::~LogDB() {
    delete db_;
}

void LogDB::store(const LogKey& key, const std::string& rawLine) {
    std::string encodedKey = serializeKey(key);

    try {
        json parsed = json::parse(rawLine);
        std::vector<std::uint8_t> msgpack_data = json::to_msgpack(parsed);

        if (db_) {
            rocksdb::Status s = db_->Put(
                rocksdb::WriteOptions(),
                encodedKey,
                rocksdb::Slice(reinterpret_cast<const char*>(msgpack_data.data()), msgpack_data.size())
            );

            std::cout << "[STORE] session=" << std::get<0>(key)
                      << " timestamp=" << std::get<1>(key)
                      << " type_size=" << std::get<2>(key).size()
                      << " status=" << s.ToString() << std::endl;

            if (!s.ok()) {
                std::cerr << "Failed to save DB: " << s.ToString() << std::endl;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to parse JSON or convert to MsgPack: " << e.what() << std::endl;
    }
}

void LogDB::read(const LogKey& key) {
    std::string encodedKey = serializeKey(key);

    if (!db_) return;
    std::string val;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), encodedKey, &val);

    if (!s.ok()) {
        std::cerr << "[READ FAIL] session=" << std::get<0>(key)
                  << " timestamp=" << std::get<1>(key)
                  << " type_size=" << std::get<2>(key).size()
                  << " status=" << s.ToString() << std::endl;
        return;
    }

    try {
        json parsed = json::from_msgpack(val);
        std::cout << "[READ] session=" << std::get<0>(key)
                  << " timestamp=" << std::get<1>(key)
                  << " data=" << parsed.dump() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[READ ERROR] Failed to decode MsgPack: " << e.what() << std::endl;
    }
}

void LogDB::compact() {
    if (db_) {
        std::cout << "[COMPACTION] Manual compaction triggered." << std::endl;
        db_->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
    }
}
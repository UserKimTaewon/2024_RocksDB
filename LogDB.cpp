#include "LogDB.h"
#include <iostream>
#include <sstream>
#include <msgpack.hpp>
#include <memory>
#include "ChronobreakFilter.h"

LogDB::LogDB(const std::string& path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compaction_filter_factory = std::make_shared<ChronobreakFilterFactory>();

    std::unique_ptr<rocksdb::DB> dbptr;
    rocksdb::Status status = rocksdb::DB::Open(options, path, &dbptr);
    if (!status.ok()) {
        std::cerr << "Failed to open RocksDB: " << status.ToString() << std::endl;
        db_ = nullptr;
    } else {
        db_ = dbptr.release();
    }
}

LogDB::~LogDB() {
    delete db_;
}

void LogDB::store(const std::string& key, const std::string& rawLine) {
    LogRecord record { rawLine };
    std::stringstream buffer;
    msgpack::pack(buffer, record);

    if (db_) {
        // debug code
        std::cout << "[STORE] key = " << key << std::endl;

        rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), key, buffer.str());
        if (!s.ok()) {
            std::cerr << "Failed to save DB: " << s.ToString() << std::endl;
        }
    }
}

void LogDB::read(const std::string& key) {
    if (!db_) return;
    std::string val;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &val);
    if (!s.ok()) {
        std::cerr << "Failed to read DB: " << s.ToString() << std::endl;
        return;
    }

    msgpack::object_handle oh = msgpack::unpack(val.data(), val.size());
    LogRecord record;
    oh.get().convert(record);

    // debug code
    std::cout << "[" << key << "] " << record.raw_line << std::endl;
}

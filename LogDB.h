#pragma once
#include <rocksdb/db.h>
#include "LogRecord.h"
#include "Key.h"

class LogDB {
public:
    explicit LogDB(const std::string& path);
    ~LogDB();

    void store(const LogKey& key, const std::string& rawLine);
    void read(const LogKey& key);
    void compact();

private:
    rocksdb::DB* db_ = nullptr;
};

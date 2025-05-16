#pragma once
#include <rocksdb/db.h>
#include "LogRecord.h"

class LogDB {
public:
    explicit LogDB(const std::string& path);
    ~LogDB();

    void store(const std::string& key, const std::string& rawLine);
    void read(const std::string& key);

private:
    rocksdb::DB* db_ = nullptr;
};
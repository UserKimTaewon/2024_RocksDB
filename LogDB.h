#pragma once
#include <rocksdb/db.h>
#include "Key.h"
#include "LogDBIterator.h"

class LogDB {
public:

    explicit LogDB(const std::string& path);
    ~LogDB();
    timestamp_t lastTimeStamp(sessionid_t sessionid);
    rocksdb::Status ChronoBreak(sessionid_t sessionid,timestamp_t rollback_diff);
    rocksdb::Status ChronoBreakAbsolute(sessionid_t sessionid,timestamp_t rollback_time);

    void store(const LogKey& key, const std::string& rawLine);
    void read(const LogKey& key);
    
    std::string ReadMSGPack(const LogKey& key);

    LogDBIterator * NewIterator();
    LogDBBoundedIterator * NewBoundedIterator(sessionid_t sessionid);

    void compact();
    
private:
    rocksdb::DB* db_ = nullptr;
};

#include "LogDB.h"
#include <iostream>
#include <sstream>
//#include <msgpack.hpp>
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

timestamp_t LogDB::lastTimeStamp(sessionid_t sessionid){
  char last_key_guard[KEY_MIN_SIZE];
  rocksdb::Slice last_key(last_key_guard,KEY_MIN_SIZE);
  fill_key(sessionid+1,0,reinterpret_cast<uint8_t *>(last_key_guard));
  rocksdb::ReadOptions read_option=rocksdb::ReadOptions();
  read_option.allow_unprepared_value=true;
  
  auto it=db_->NewIterator(read_option);

  it->SeekForPrev(last_key);
  assert(it->Valid());
  auto key=it->key();
  timestamp_t timestamp= std::get<1>(deserializeKey(key));
  delete it;
  return timestamp;
}

rocksdb::Status LogDB::ChronoBreak(sessionid_t sessionid,timestamp_t rollback_diff){
    timestamp_t last_timestamp=lastTimeStamp(sessionid);
    return ChronoBreakAbsolute(sessionid,last_timestamp-rollback_diff);
}

rocksdb::Status LogDB::ChronoBreakAbsolute(sessionid_t sessionid,timestamp_t rollback_time){
char begin_key_guard[KEY_MIN_SIZE],end_key_guard[KEY_MIN_SIZE];
  rocksdb::Slice begin_key(begin_key_guard,KEY_MIN_SIZE),end_key(end_key_guard,KEY_MIN_SIZE);
  fill_key(sessionid,rollback_time,reinterpret_cast<uint8_t *> (begin_key_guard));
  fill_key(sessionid+1,0,reinterpret_cast<uint8_t *>( end_key_guard));
  return db_->DeleteRange(rocksdb::WriteOptions(),begin_key,end_key);
}

std::string LogDB::ReadMSGPack(const LogKey& key){
    if (!db_) throw std::runtime_error("no db");;
    std::string encodedKey = serializeKey(key);
    std::string val;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), encodedKey, &val);

    if (!s.ok()) {
        std::cerr << "[READ FAIL] session=" << std::get<0>(key)
                  << " timestamp=" << std::get<1>(key)
                  << " type_size=" << std::get<2>(key).size()
                  << " status=" << s.ToString() << std::endl;
        throw std::runtime_error("read fail");
    }
    return val;
}

LogDBIterator * LogDB::NewIterator(){
    auto it=db_->NewIterator(rocksdb::ReadOptions());
    return new LogDBIterator(it);

}
LogDBBoundedIterator * LogDB::NewBoundedIterator(sessionid_t sessionid){
    return new LogDBBoundedIterator(db_,sessionid);
}
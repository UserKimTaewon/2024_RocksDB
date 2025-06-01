#include <rocksdb/iterator.h>
#include "Key.h"
#include <nlohmann/json.hpp>

struct LogDBIterator{
    friend class LogDB;
public:

    virtual ~LogDBIterator()
    {
        delete it;
    }
    bool Valid()
    {
        return it->Valid();
    }
    void Next()
    {
        return it->Next();
    }
    void Prev()
    {
        return it->Prev();
    }
    virtual void SeekToFirst()
    {
        it->SeekToFirst();
    }
    virtual void SeekToLast()
    {
        it->SeekToLast();
    }

    rocksdb::Status status()
    {
        return it->status();
    }
    LogKey key()
    {
        return deserializeKey(it->key());
    }

    rocksdb::Slice valueMSGPack()
    {
        return it->value();
    }
    nlohmann::json valueJSONDocument()
    {
        auto val = it->value();
        return nlohmann::json::from_msgpack(val.data(), val.data() + val.size());
    }

protected:
    explicit LogDBIterator(rocksdb::Iterator *iter): it(iter) {}
    rocksdb::Iterator *it;
    LogDBIterator():it(nullptr) {}
};
struct LogDBBoundedIterator : LogDBIterator
{
    friend class LogDB;
public:
    virtual void SeekToFirst()
    {
        it->Seek(begin_key);
    }
    virtual void SeekToLast()
    {
        it->SeekForPrev(end_key);
    }

protected:
    char begin_key_guard[KEY_MIN_SIZE],end_key_guard[KEY_MIN_SIZE];
    rocksdb::Slice begin_key, end_key;
    LogDBBoundedIterator(rocksdb::DB *db,
                         sessionid_t start_sessionid,
                         sessionid_t end_sessionid):
        begin_key(begin_key_guard, KEY_MIN_SIZE),
        end_key(end_key_guard, KEY_MIN_SIZE)
    {
        fill_key(start_sessionid, 0, reinterpret_cast<uint8_t *>(begin_key_guard));
        fill_key(end_sessionid + 1, 0, reinterpret_cast<uint8_t *>(end_key_guard));

        rocksdb::ReadOptions read_option=rocksdb::ReadOptions();
        read_option.iterate_lower_bound = &begin_key;
        read_option.iterate_upper_bound = &end_key;
        it = db->NewIterator(read_option);
    }
    LogDBBoundedIterator(rocksdb::DB * db,
                         sessionid_t sessinid):LogDBBoundedIterator(db, sessinid, sessinid) {}
};
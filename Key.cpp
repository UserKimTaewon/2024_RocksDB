#include "Key.h"
#include <cstring>
#include <sstream>

void fill_key(sessionid_t sessinid,timestamp_t timestamp,uint8_t * out){
    EncodeFixed32BE(out,sessinid);
    EncodeFixed32BE(out+sizeof(sessinid),timestamp);
}

void serializeKey(const LogKey& key,std::string& out) {
    std::string& result=out;
    uint32_t session_id = std::get<0>(key);
    uint32_t timestamp = std::get<1>(key);
    auto type = std::get<2>(key);
    uint8_t buf[KEY_MIN_SIZE];
    fill_key(session_id,timestamp,buf);

    //result.append(reinterpret_cast<const char*>(&session_id), sizeof(session_id));
    //result.append(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
    result.append(reinterpret_cast<const char*>(buf),sizeof(buf));
    result.append(reinterpret_cast<const char*>(type.data()), type.size());

}

std::string serializeKey(const LogKey& key) {
    std::string result;
    serializeKey(key,result);
    return result;
}
LogKey deserializeKey(const std::string& raw) {
    if (raw.size() < KEY_MIN_SIZE) {
        throw std::runtime_error("Key too short to decode");
    }

    uint32_t session_id, timestamp;
    //std::memcpy(&session_id, raw.data(), 4);
    //std::memcpy(&timestamp, raw.data() + 4, 4);
    auto ptr=reinterpret_cast<const uint8_t*>(raw.data());
    session_id=DecodeFixed32BE(ptr);
    timestamp=DecodeFixed32BE(ptr+4);

    std::string type(raw.begin() + 8, raw.end());

    return { session_id, timestamp, type };
}
LogKey deserializeKey(const rocksdb::Slice key){
    if (key.size() < KEY_MIN_SIZE) {
        throw std::runtime_error("Key too short to decode");
    }

    uint32_t session_id, timestamp;
    //std::memcpy(&session_id, raw.data(), 4);
    //std::memcpy(&timestamp, raw.data() + 4, 4);
    auto ptr=reinterpret_cast<const uint8_t*>(key.data());
    session_id=DecodeFixed32BE(ptr);
    timestamp=DecodeFixed32BE(ptr+4);

    std::string type(ptr + 8, ptr+key.size());

    return { session_id, timestamp, type };

}
#include "Key.h"
#include <cstring>
#include <sstream>

std::string serializeKey(const LogKey& key) {
    std::string result;
    uint32_t session_id = std::get<0>(key);
    uint32_t timestamp = std::get<1>(key);
    const std::vector<uint8_t>& type = std::get<2>(key);

    result.append(reinterpret_cast<const char*>(&session_id), sizeof(session_id));
    result.append(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
    result.append(reinterpret_cast<const char*>(type.data()), type.size());

    return result;
}

LogKey deserializeKey(const std::string& raw) {
    if (raw.size() < 8) {
        throw std::runtime_error("Key too short to decode");
    }

    uint32_t session_id, timestamp;
    std::memcpy(&session_id, raw.data(), 4);
    std::memcpy(&timestamp, raw.data() + 4, 4);

    std::vector<uint8_t> type(raw.begin() + 8, raw.end());

    return { session_id, timestamp, type };
}

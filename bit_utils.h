#pragma once
#include <cstdint>
#include <cstring>

inline void EncodeFixed32BE(uint8_t * buf, uint32_t value) {
    buf[0] = (value >> 24) & 0xff;
    buf[1] = (value >> 16) & 0xff;
    buf[2] = (value >> 8) & 0xff;
    buf[3] = value & 0xff;
}

inline void EncodeFixed64BE(uint8_t * buf, uint64_t value) {
    buf[0] = (value >> 56) & 0xff;
    buf[1] = (value >> 48) & 0xff;
    buf[2] = (value >> 40) & 0xff;
    buf[3] = (value >> 32) & 0xff;
    buf[4] = (value >> 24) & 0xff;
    buf[5] = (value >> 16) & 0xff;
    buf[6] = (value >> 8) & 0xff;
    buf[7] = value & 0xff;

}
inline uint32_t DecodeFixed32BE(const uint8_t * ptr) {
    return ((static_cast<uint32_t>((ptr[0])) << 24)|
            (static_cast<uint32_t>((ptr[1])) << 16)|
            (static_cast<uint32_t>((ptr[2])) << 8) |
			(static_cast<uint32_t>((ptr[3]))));

}

inline uint64_t DecodeFixed64BE(const uint8_t* ptr) {
    uint64_t hi = DecodeFixed32BE(ptr);
    uint64_t lo = DecodeFixed32BE(ptr+4);
    return (hi << 32) | lo;
}


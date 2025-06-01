#pragma once
#include <tuple>
#include <vector>
#include <string>
#include <cstdint>
#include <rocksdb/slice.h>
#include "bit_utils.h"

using timestamp_t=uint32_t;
using sessionid_t=uint32_t;
using LogKey = std::tuple<sessionid_t,timestamp_t, std::vector<uint8_t>>;
const int KEY_MIN_SIZE=8;
static_assert(KEY_MIN_SIZE==sizeof(sessionid_t)+sizeof(timestamp_t));
static_assert(sizeof(sessionid_t)==4);
static_assert(sizeof(timestamp_t)==4);

std::string serializeKey(const LogKey& key);
void serializeKey(const LogKey & key,std::string& out);
LogKey deserializeKey(const std::string& key);
LogKey deserializeKey(const ROCKSDB_NAMESPACE::Slice key);

void fill_key(sessionid_t sessinid,timestamp_t timestamp,uint8_t * out);
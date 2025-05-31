#pragma once
#include <tuple>
#include <vector>
#include <string>
#include <cstdint>

using LogKey = std::tuple<int64_t, int64_t, std::vector<uint8_t>>;

std::string serializeKey(const LogKey& key);
LogKey deserializeKey(const std::string& key);

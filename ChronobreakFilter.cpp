#include "ChronobreakFilter.h"

ChronobreakFilter::ChronobreakFilter() {}

bool ChronobreakFilter::Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value,
                                std::string* new_value, bool* value_changed) const {
  std::string key_str = key.ToString();
  std::regex pattern(R"(\d+,\s*(\d+),)");
  std::smatch match;

  if (!std::regex_search(key_str, match, pattern)) return false;

  int64_t timestamp = std::stoll(match[1]);
  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch()).count();

    // Check if the timestamp is within the last 15 minutes
  if (now_ms - timestamp < FIFTEEN_MIN_MS) {
    return false;
  }

    // Check if the timestamp is a multiple of 60 seconds
  int64_t seconds = timestamp / 1000;
  if (seconds % 60 != 0) {
    return true;  // 삭제
  }

  return false;
}

const char* ChronobreakFilter::Name() const {
  return "ChronobreakFilter";
}

std::unique_ptr<rocksdb::CompactionFilter> ChronobreakFilterFactory::CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) {
  return std::make_unique<ChronobreakFilter>();
}

const char* ChronobreakFilterFactory::Name() const {
  return "ChronobreakFilterFactory";
}

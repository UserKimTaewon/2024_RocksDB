#include "ChronobreakFilter.h"
#include "Key.h"

ChronobreakFilter::ChronobreakFilter() {}

bool ChronobreakFilter::Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value,
                                std::string* new_value, bool* value_changed) const {
  LogKey key_data=deserializeKey(key);
  int64_t timestamp =std::get<1>(key_data);

  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch()).count();
  
  if (now_ms - timestamp > FIFTEEN_SEC_MS) {
    std::cout << "[FILTER] Dropping key due to TTL_X: " << std::get<2>(key_data) << std::endl;
    return true;
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

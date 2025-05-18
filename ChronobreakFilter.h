#pragma once
#include <rocksdb/compaction_filter.h>
#include <chrono>
#include <regex>
#include <string>
#include <memory>

class ChronobreakFilter : public rocksdb::CompactionFilter {
 public:
  ChronobreakFilter();
  bool Filter(int level, const rocksdb::Slice& key, const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override;
  const char* Name() const override;

 private:
  static constexpr int64_t FIFTEEN_MIN_MS = 15 * 1000;
};

class ChronobreakFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override;
  const char* Name() const override;
};

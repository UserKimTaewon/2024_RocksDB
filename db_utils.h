#pragma once
#include "LogDB.h"
#include <nlohmann/json.hpp>
#include <iomanip>

void dump_iterator(LogDBIterator *it, std::ofstream &out){
  for (it->SeekToFirst(); it->Valid(); it->Next()){
    LogKey key = it->key();
    out<<"{\"" << std::get<0>(key) << ',' << std::get<1>(key) << ',';
    auto type = std::get<2>(key);
    for(auto i:type){
      out<<static_cast<char>(i);
    }
    out<<"\":";
    out << it->valueJSONDocument();
    out << "}\n";
  }
  delete it;
}

void dump_to_jsonl(LogDB *db, sessionid_t sessionid, std::ofstream &out){
  auto it = db->NewBoundedIterator(sessionid);
  dump_iterator(it, out);
}

void dump_to_jsonl(LogDB *db, std::ofstream &out){
  auto it = db->NewIterator();
  dump_iterator(it, out);
}
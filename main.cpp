#include "LogDB.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <regex>
#include <vector>
#include <tuple>
#include <thread>
#include <chrono>
#include <algorithm>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

long long extractTimestampFromKey(const std::string& key) {
    std::regex pattern(R"(\s*\"?\s*\d+\s*,\s*(\d+)\s*,)");
    std::smatch match;
    if (std::regex_search(key, match, pattern)) {
        return std::stoll(match[1]);
    }
    return -1;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << "Insert a number in 0 to 4" << std::endl;
        return 1;
    }

    int fileIndex = std::stoi(argv[1]);
    if (fileIndex < 0 || fileIndex > 4) {
        std::cerr << "File number must be in 0 to 4" << std::endl;
        return 1;
    }

    std::string filename = "logfiles/log_" + std::to_string(fileIndex) + ".jsonl";
    std::ifstream infile(filename);
    if (!infile.is_open()) {
        std::cerr << "Can't open the file: " << filename << std::endl;
        return 1;
    }

    LogDB db("logdb");
    std::vector<std::tuple<long long, std::string, std::string>> entries;

    std::string line;
    while (std::getline(infile, line)) {
        json parsed = json::parse(line);
        for (auto& [key, value] : parsed.items()) {
            long long ts = extractTimestampFromKey(key);
            if (ts != -1) {
                entries.emplace_back(ts, key, value.dump());
            }
        }
    }

    std::sort(entries.begin(), entries.end());

    for (size_t i = 0; i < entries.size(); ++i) {
        if (i > 0) {
            long long delta = std::get<0>(entries[i]) - std::get<0>(entries[i - 1]);
            double wait_time = static_cast<double>(delta) / 1000.0 / 20.0;
            std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(wait_time * 1000)));
        }

        const std::string& key = std::get<1>(entries[i]);
        const std::string& value = std::get<2>(entries[i]);
        db.store(key, value);
    }

    std::cout << "Logs in top 50:\n";
    for (size_t i = 0; i < std::min<size_t>(entries.size(), 50); ++i) {
        db.read(std::get<1>(entries[i]));
    }

    sleep(20);
    db.compact();
    std::cout << "Compaction done." << std::endl;
    return 0;
}

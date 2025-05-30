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
#include <fstream>

using json = nlohmann::json;

long long extractTimestampFromKey(const std::string& key) {
    std::regex pattern(R"(\s*\"?\s*\d+\s*,\s*(\d+)\s*,)");
    std::smatch match;
    if (std::regex_search(key, match, pattern)) {
        return std::stoll(match[1]);
    }
    return -1;
}

LogKey parseKey(const std::string& raw_key_str) {
    std::regex pattern(R"(\s*\"?\s*(\d+)\s*,\s*(\d+)\s*,\s*(\w+)\s*\"?)");
    std::smatch match;
    if (!std::regex_search(raw_key_str, match, pattern)) {
        throw std::runtime_error("Invalid key format: " + raw_key_str);
    }

    int64_t session_id = std::stoll(match[1].str());
    int64_t timestamp = std::stoll(match[2].str());
    std::string type_str = match[3].str();

    std::vector<uint8_t> type_bytes(type_str.begin(), type_str.end());

    return { session_id, timestamp, type_bytes };
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
    std::vector<std::pair<LogKey, std::string>> entries;

    std::string line;
    while (std::getline(infile, line)) {
        json parsed = json::parse(line);
        for (auto& [key_str, value] : parsed.items()) {
            try {
                LogKey key = parseKey(key_str);
                entries.emplace_back(key, value.dump());
            } catch (const std::exception& e) {
                std::cerr << "Failed to parse key: " << e.what() << std::endl;
            }
        }
    }

    std::sort(entries.begin(), entries.end(),
    [](const auto& a, const auto& b) {
        return std::get<1>(a.first) < std::get<1>(b.first);
    });

    std::ofstream storeLogFile("store_log_i_0_to_600.txt");

    bool chronoBreakApplied = false;
    int64_t chronoShift = 0;

    for (size_t i = 0; i < entries.size(); ++i) {
        if (i == 300) {
            std::cout << "[CHRONO BREAK] applying -5min at i = " << i << std::endl;
            int64_t original_ts = std::get<1>(entries[i].first);
            std::get<1>(entries[i].first) -= 300000;
            chronoShift = original_ts - std::get<1>(entries[i].first);
            chronoBreakApplied = true;
        } else if (chronoBreakApplied) {
            std::get<1>(entries[i].first) -= chronoShift;
        }

        if (i > 0) {
            int64_t delta = std::get<1>(entries[i].first) - std::get<1>(entries[i - 1].first);
            if (delta > 0) {
                double wait_time = static_cast<double>(delta) / 1000.0 / 20.0;
                std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(wait_time * 1000)));
            } else {
                std::cout << "[SKIP SLEEP] Negative delta at i=" << i << ", delta=" << delta << std::endl;
            }
        }

        const LogKey& key = entries[i].first;
        const std::string& value = entries[i].second;

        std::cout << "[SEND] i=" << i
                  << " session=" << std::get<0>(key)
                  << " timestamp=" << std::get<1>(key)
                  << " value=" << value.substr(0, 60) << "..." << std::endl;

        if (i <= 600 && storeLogFile.is_open()) {
            storeLogFile << "i=" << i
                << " session=" << std::get<0>(key)
                << " timestamp=" << std::get<1>(key)
                << " type_size=" << std::get<2>(key).size()
                << std::endl;
        }

        db.store(key, value);
    }

    storeLogFile.close();

    std::cout << "\n[READ CHECK] Entries around chrono break:\n";
    for (size_t i = 295; i <= 305 && i < entries.size(); ++i) {
        db.read(entries[i].first);
    }

    std::cout << "\n[FINAL READ] Logs in top 50:\n";
    for (size_t i = 0; i < std::min<size_t>(entries.size(), 50); ++i) {
        db.read(entries[i].first);
    }

    sleep(20);
    db.compact();
    std::cout << "Compaction done." << std::endl;
    return 0;
}

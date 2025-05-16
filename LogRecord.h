#pragma once
#include <string>
#include <msgpack.hpp>

struct LogRecord {
    std::string raw_line;

    MSGPACK_DEFINE(raw_line);
};
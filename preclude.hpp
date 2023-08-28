#pragma once

#include "external/expected.hpp"
#include "external/log.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <span>
#include <string>
#include <utility>
#include <vector>


namespace ext {
using namespace tl;
}

template <std::invocable F>
struct Defer {
  Defer(F&& f) : f(std::forward<F>(f)) {}
  ~Defer() { f(); }
  F f;
};


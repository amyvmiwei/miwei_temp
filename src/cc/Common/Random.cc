/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * Random number generator for int32, int64, double and ascii arrays.
 */

#include <Common/Compat.h>

#include "Random.h"

#include <cassert>
#include <mutex>
#include <random>

#if defined(__APPLE__)
#define THREAD_LOCAL
#define LOCK_GLOBAL_MUTEX(m) std::lock_guard<std::mutex> lock(m)
#else
#define THREAD_LOCAL thread_local
#define LOCK_GLOBAL_MUTEX(m) (void)m
#endif

using namespace Hypertable;
using namespace std;

namespace {
  THREAD_LOCAL mt19937 g_random_engine {1};
  std::mutex g_mutex;
}

void Random::seed(unsigned int s) {
  LOCK_GLOBAL_MUTEX(g_mutex);
  g_random_engine.seed(s);
}

uint32_t Random::number32(uint32_t maximum) {
  LOCK_GLOBAL_MUTEX(g_mutex);
  if (maximum) {
    return uniform_int_distribution<uint32_t>(0, maximum-1)(g_random_engine);
  }
  return uniform_int_distribution<uint32_t>()(g_random_engine);
}

int64_t Random::number64(int64_t maximum) {
  LOCK_GLOBAL_MUTEX(g_mutex);
  if (maximum) {
    assert(maximum > 0);
    return uniform_int_distribution<int64_t>(0, maximum-1)(g_random_engine);
  }
  return uniform_int_distribution<int64_t>()(g_random_engine);
}

double Random::uniform01() {
  LOCK_GLOBAL_MUTEX(g_mutex);
  return uniform_real_distribution<>()(g_random_engine);
}

chrono::milliseconds Random::duration_millis(uint32_t maximum) {
  LOCK_GLOBAL_MUTEX(g_mutex);
  assert(maximum > 0);
  uniform_int_distribution<uint32_t> di(0, maximum-1);
  return chrono::milliseconds(di(g_random_engine));
}

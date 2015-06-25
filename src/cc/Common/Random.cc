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

namespace {
  const char cb64[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
}

using namespace Hypertable;
using namespace std;

thread_local mt19937 Random::ms_random_engine {1};

uint32_t Random::number32(uint32_t maximum) {
  if (maximum) {
    return uniform_int_distribution<uint32_t>(0, maximum-1)(ms_random_engine);
  }
  return uniform_int_distribution<uint32_t>()(ms_random_engine);
}

int64_t Random::number64(int64_t maximum) {
  if (maximum) {
    assert(maximum > 0);
    return uniform_int_distribution<int64_t>(0, maximum-1)(ms_random_engine);
  }
  return uniform_int_distribution<int64_t>()(ms_random_engine);
}

double Random::uniform01() {
  return uniform_real_distribution<>()(ms_random_engine);
}

chrono::milliseconds Random::duration_millis(uint32_t maximum) {
  assert(maximum > 0);
  uniform_int_distribution<uint32_t> di(0, maximum-1);
  return chrono::milliseconds(di(ms_random_engine));
}

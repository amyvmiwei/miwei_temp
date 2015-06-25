/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#include <Common/Compat.h>

#include "DataGeneratorRandom.h"

#include <cassert>
#include <random>

namespace {
  const char cb64[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::mt19937 g_engine {1};
}

int32_t Hypertable::random_int32(int32_t maximum) {
  assert(maximum > 0);
  return std::uniform_int_distribution<int32_t>(0, maximum-1)(g_engine);
}

void Hypertable::random_generator_set_seed(unsigned seed) {
  g_engine.seed(seed);
}

void Hypertable::random_fill_with_chars(char *buf, size_t len,
                                        const char *charset) {
  size_t in_i = 0, out_i = 0;
  uint32_t u32;
  uint8_t *in;
  const char *chars;

  if (charset)
    chars = charset;
  else
    chars = cb64;
  size_t set_len = strlen(chars);

  assert(set_len > 0 && set_len <= 256);

  while (out_i < len) {
    u32 = std::uniform_int_distribution<uint32_t>()(g_engine);
    in = (uint8_t *)&u32;

    in_i = 0;
    buf[out_i++] = chars[in[in_i] % set_len];
    if (out_i == len)
      break;

    in_i++;
    buf[out_i++] = chars[in[in_i] % set_len];
    if (out_i == len)
      break;

    in_i++;
    buf[out_i++] = chars[in[in_i] % set_len];
    if (out_i == len)
      break;

    in_i++;
    buf[out_i++] = chars[in[in_i] % set_len];
  }
}

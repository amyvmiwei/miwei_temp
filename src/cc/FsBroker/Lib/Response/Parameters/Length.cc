/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Definitions for Length response parameters.
/// This file contains definitions for Length, a class for encoding and
/// decoding paramters to the <i>length</i> file system broker function.

#include <Common/Compat.h>

#include "Length.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::FsBroker::Lib::Response::Parameters;

namespace {
  uint8_t VERSION {1};
}

size_t Length::encoded_length() const {
  size_t length = internal_encoded_length();
  return 1 + Serialization::encoded_length_vi32(length) + length;
}

void Length::encode(uint8_t **bufp) const {
  Serialization::encode_i8(bufp, VERSION);
  Serialization::encode_vi32(bufp, internal_encoded_length());
  Serialization::encode_i64(bufp, m_length);
}

void Length::decode(const uint8_t **bufp, size_t *remainp) {
  uint8_t version = Serialization::decode_i8(bufp, remainp);
  if (version != VERSION)
    HT_THROWF(Error::PROTOCOL_ERROR,
	      "Length parameters version mismatch, expected %d, got %d",
	      (int)VERSION, (int)version);
  uint32_t encoding_length = Serialization::decode_vi32(bufp, remainp);
  const uint8_t *end = *bufp + encoding_length;
  m_length = Serialization::decode_i64(bufp, remainp);
  // If encoding is longer than we expect, that means we're decoding a newer
  // version, so skip the newer portion that we don't know about
  if (*bufp < end)
    *bufp = end;
}

size_t Length::internal_encoded_length() const {
  return 8;
}

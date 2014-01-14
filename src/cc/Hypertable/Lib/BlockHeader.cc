/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Definitions for BlockHeader.
 * This file contains definitions for BlockHeader, a base class that holds
 * common block header fields.
 */

#include <Common/Compat.h>

#include "BlockHeader.h"

#include <Common/Serialization.h>
#include <Common/Checksum.h>
#include <Common/Error.h>
#include <Common/Logger.h>

#include <Hypertable/Lib/BlockCompressionCodec.h>

#include <cstring>

using namespace Hypertable;
using namespace Serialization;

namespace {
  const size_t VersionLengths[BlockHeader::LatestVersion+1] = { 26, 28 };
}

const uint16_t BlockHeader::LatestVersion;

BlockHeader::BlockHeader(uint16_t version, const char *magic) :
  m_flags(0), m_data_length(0), m_data_zlength(0), m_data_checksum(0),
  m_compression_type((uint16_t)-1), m_version(version) {
  HT_ASSERT(version <= LatestVersion);
  if (magic)
    memcpy(m_magic, magic, 10);
  else
    memset(m_magic, 0, 10);
}


void
BlockHeader::write_header_checksum(uint8_t *base) {

  // Original format put the checksum at the end
  if (m_version == 0) {
    uint8_t *buf = base + encoded_length() - 2;
    encode_i16(&buf, fletcher32(base, encoded_length()-2));
    return;
  }

  uint8_t *buf = base + 10;
  encode_i16(&buf, fletcher32(base+12, encoded_length()-12));
}


size_t BlockHeader::encoded_length() {
  return VersionLengths[m_version];
}

/**
 */
void BlockHeader::encode(uint8_t **bufp) {

  HT_ASSERT(m_compression_type < BlockCompressionCodec::COMPRESSION_TYPE_LIMIT);

  memcpy(*bufp, m_magic, 10);
  (*bufp) += 10;

  if (m_version != 0) {
    (*bufp) += 2;  // space for checksum
    encode_i16(bufp, m_flags);
  }

  *(*bufp)++ = (uint8_t)encoded_length();
  *(*bufp)++ = (uint8_t)m_compression_type;
  encode_i32(bufp, m_data_checksum);
  encode_i32(bufp, m_data_length);
  encode_i32(bufp, m_data_zlength);
}


/**
 */
void BlockHeader::decode(const uint8_t **bufp, size_t *remainp) {
  const uint8_t *base = *bufp;

  if (*remainp < encoded_length())
    HT_THROW(Error::BLOCK_COMPRESSOR_TRUNCATED, "");

  memcpy(m_magic, *bufp, 10);
  (*bufp) += 10;
  *remainp -= 10;

  if (m_version != 0) {
    uint16_t header_checksum = decode_i16(bufp, remainp);
    uint16_t header_checksum_computed = fletcher32(base+12, encoded_length()-12);
    if (header_checksum_computed != header_checksum)
      HT_THROWF(Error::BLOCK_COMPRESSOR_BAD_HEADER,
                "Header checksum mismatch: %u (computed) != %u (stored)",
                (unsigned)header_checksum_computed, (unsigned)header_checksum);
    m_flags = decode_i16(bufp, remainp);
  }

  uint16_t header_length = decode_byte(bufp, remainp);

  if (header_length != encoded_length())
    HT_THROWF(Error::BLOCK_COMPRESSOR_BAD_HEADER, "Unexpected header length"
              ": %lu, expecting: %lu", (Lu)header_length, (Lu)encoded_length());

  m_compression_type = decode_byte(bufp, remainp);

  if (m_compression_type >= BlockCompressionCodec::COMPRESSION_TYPE_LIMIT)
    HT_THROWF(Error::BLOCK_COMPRESSOR_BAD_HEADER,
              "Unsupported compression type (%d)", (int)m_compression_type);

  m_data_checksum = decode_i32(bufp, remainp);
  m_data_length = decode_i32(bufp, remainp);
  m_data_zlength = decode_i32(bufp, remainp);

}


bool BlockHeader::equals(const BlockHeader &other) const {
  if (m_version == other.m_version &&
      !memcmp(m_magic, other.m_magic, 10) &&
      m_data_length == other.m_data_length &&
      m_data_zlength == other.m_data_zlength &&
      m_data_checksum == other.m_data_checksum) {
    if (m_version > 0)
      return m_flags == other.m_flags;
    return true;
  }
  return false;
}

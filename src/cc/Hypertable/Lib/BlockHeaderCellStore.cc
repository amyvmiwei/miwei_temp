/*
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
 * Definitions for BlockHeaderCellStore.
 * This file contains definitions for BlockHeaderCellStore, a base class that
 * holds cell store block header fields.
 */

#include <Common/Compat.h>

#include "BlockHeaderCellStore.h"

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Serialization;

namespace {
  const size_t EncodedLengths[BlockHeaderCellStore::LatestVersion+1] = { 0, 0 };
  const uint16_t BaseVersions[BlockHeaderCellStore::LatestVersion+1] = { 0, 1 };
}

const uint16_t BlockHeaderCellStore::LatestVersion;

BlockHeaderCellStore::BlockHeaderCellStore(uint16_t version, const char *magic) :
  BlockHeader(BaseVersions[version], magic), m_version(version) {
  HT_ASSERT(version <= LatestVersion);
}

size_t BlockHeaderCellStore::encoded_length() {
  return BlockHeader::encoded_length() + EncodedLengths[m_version];
}


void BlockHeaderCellStore::encode(uint8_t **bufp) {
  uint8_t *base = *bufp;
  BlockHeader::encode(bufp);
  if (m_version == 0)
    *bufp += 2;
  write_header_checksum(base);
}


void BlockHeaderCellStore::decode(const uint8_t **bufp, size_t *remainp) {

  BlockHeader::decode(bufp, remainp);

  // If version 0, skip old checksum field
  if (m_version == 0) {
    *bufp += 2;
    *remainp -= 2;
    return;
  }
}



bool BlockHeaderCellStore::equals(const BlockHeaderCellStore &other) const {
  if (static_cast<const BlockHeader &>(*this) == other &&
      m_version == other.m_version)
    return true;
  return false;
}

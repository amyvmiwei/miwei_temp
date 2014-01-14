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
 * Definitions for BlockHeaderCommitLog.
 * This file contains definitions for BlockHeaderCommitLog, a base class that
 * holds commit log block header fields.
 */

#include <Common/Compat.h>

#include "BlockHeaderCommitLog.h"

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Serialization;

namespace {
  const size_t EncodedLengths[BlockHeaderCommitLog::LatestVersion+1] = { 8, 16 };
  const uint16_t BaseVersions[BlockHeaderCommitLog::LatestVersion+1] = { 0, 1 };
}

const uint16_t BlockHeaderCommitLog::LatestVersion;

BlockHeaderCommitLog::BlockHeaderCommitLog(uint16_t version) :
  BlockHeader(BaseVersions[version]), m_revision(0), m_cluster_id(0),
  m_version(version) {
  HT_ASSERT(version <= LatestVersion);
}

BlockHeaderCommitLog::BlockHeaderCommitLog(const char *magic,int64_t revision,
                                           uint64_t cluster_id) :
  BlockHeader(BaseVersions[LatestVersion], magic), m_revision(revision),
  m_cluster_id(cluster_id), m_version(LatestVersion) {
}


size_t BlockHeaderCommitLog::encoded_length() {
  return BlockHeader::encoded_length() + EncodedLengths[m_version];
}


void BlockHeaderCommitLog::encode(uint8_t **bufp) {
  uint8_t *base = *bufp;
  BlockHeader::encode(bufp);
  encode_i64(bufp, m_revision);
  if (m_version == 0)
    *bufp += 2;
  else
    encode_i64(bufp, m_cluster_id);
  write_header_checksum(base);
}


void BlockHeaderCommitLog::decode(const uint8_t **bufp,
                                  size_t *remainp) {

  BlockHeader::decode(bufp, remainp);
  m_revision = decode_i64(bufp, remainp);

  // If version 0, skip old checksum field
  if (m_version == 0) {
    *bufp += 2;
    *remainp -= 2;
    return;
  }

  m_cluster_id = decode_i64(bufp, remainp);    
}


bool BlockHeaderCommitLog::equals(const BlockHeaderCommitLog &other) const {
  if (static_cast<const BlockHeader &>(*this) == other &&
      m_version == other.m_version &&
      m_revision == other.m_revision) {
    if (m_version > 0)
      return m_cluster_id == other.m_cluster_id;
    return true;
  }
  return false;
}

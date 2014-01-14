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

#include <Common/Compat.h>
#include <Common/Logger.h>

#include <Hypertable/Lib/BlockCompressionCodec.h>
#include <Hypertable/Lib/BlockHeaderCellStore.h>
#include <Hypertable/Lib/BlockHeaderCommitLog.h>

using namespace Hypertable;

int main(int argc, char **argv) {
  uint8_t buffer[256];
  uint8_t *encode_ptr;
  const uint8_t *decode_ptr;
  size_t remain;

  //
  // CommitLog
  //

  {
    HT_ASSERT(BlockHeaderCommitLog::LatestVersion == 1);

    BlockHeaderCommitLog before;
    BlockHeaderCommitLog after;

    // Version 0

    encode_ptr = buffer;
    before = BlockHeaderCommitLog(0);
    before.set_compression_type(BlockCompressionCodec::NONE);
    before.set_data_length(1000);
    before.set_data_zlength(100);
    before.set_data_checksum(42);
    before.set_revision(123456789LL);
    before.encode(&encode_ptr);

    remain = encode_ptr-buffer;
    HT_ASSERT(remain == 34);
    decode_ptr = buffer;
    after = BlockHeaderCommitLog(0);
    after.decode(&decode_ptr, &remain);

    HT_ASSERT(before == after);

    // Version 1

    encode_ptr = buffer;
    before = BlockHeaderCommitLog("COMMITDATA", 123456789LL, 9876543210LLU);
    before.set_compression_type(BlockCompressionCodec::BMZ);
    before.set_data_length(1000);
    before.set_data_zlength(100);
    before.set_data_checksum(42);
    before.encode(&encode_ptr);

    remain = encode_ptr-buffer;
    HT_ASSERT(remain == 44);
    decode_ptr = buffer;
    after = BlockHeaderCommitLog(1);
    after.decode(&decode_ptr, &remain);
    
    HT_ASSERT(after.get_revision() == 123456789LL);
    HT_ASSERT(after.get_cluster_id() == 9876543210LLU);
    HT_ASSERT(after.check_magic("COMMITDATA"));

    HT_ASSERT(before == after);
  }

  //
  // CellStore
  //

  {
    HT_ASSERT(BlockHeaderCellStore::LatestVersion == 1);

    BlockHeaderCellStore before;
    BlockHeaderCellStore after;

    // Version 0

    encode_ptr = buffer;
    before = BlockHeaderCellStore(0);
    before.set_compression_type(BlockCompressionCodec::LZO);
    before.set_data_length(1000);
    before.set_data_zlength(100);
    before.set_data_checksum(42);
    before.encode(&encode_ptr);

    remain = encode_ptr-buffer;
    HT_ASSERT(remain == 26);
    decode_ptr = buffer;
    after = BlockHeaderCellStore(0);
    after.decode(&decode_ptr, &remain);

    HT_ASSERT(after.get_data_length() == 1000);
    HT_ASSERT(after.get_data_zlength() == 100);
    HT_ASSERT(after.get_data_checksum() == 42);

    HT_ASSERT(before == after);

    // Version 1

    encode_ptr = buffer;
    before = BlockHeaderCellStore(1, "CELLSTORE-");
    before.set_compression_type(BlockCompressionCodec::SNAPPY);
    before.set_data_length(1000);
    before.set_data_zlength(100);
    before.set_data_checksum(28);
    before.encode(&encode_ptr);

    remain = encode_ptr-buffer;
    HT_ASSERT(remain == 28);
    decode_ptr = buffer;
    after = BlockHeaderCellStore(1);
    after.decode(&decode_ptr, &remain);

    HT_ASSERT(before == after);
  }

  return 0;
}

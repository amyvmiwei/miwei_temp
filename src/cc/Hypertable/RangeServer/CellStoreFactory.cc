/* -*- c++ -*-
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

/// @file
/// Definitions for CellStoreFactory.
/// This file contains the type definitions for CellStoreFactory, an class that
/// provides an interface for creating CellStore objects from cell store files.

#include <Common/Compat.h>
#include "CellStoreFactory.h"

#include <Hypertable/RangeServer/CellStoreTrailerV0.h>
#include <Hypertable/RangeServer/CellStoreTrailerV1.h>
#include <Hypertable/RangeServer/CellStoreTrailerV2.h>
#include <Hypertable/RangeServer/CellStoreTrailerV3.h>
#include <Hypertable/RangeServer/CellStoreTrailerV4.h>
#include <Hypertable/RangeServer/CellStoreTrailerV5.h>
#include <Hypertable/RangeServer/CellStoreTrailerV6.h>
#include <Hypertable/RangeServer/CellStoreTrailerV7.h>
#include <Hypertable/RangeServer/CellStoreV0.h>
#include <Hypertable/RangeServer/CellStoreV1.h>
#include <Hypertable/RangeServer/CellStoreV2.h>
#include <Hypertable/RangeServer/CellStoreV3.h>
#include <Hypertable/RangeServer/CellStoreV4.h>
#include <Hypertable/RangeServer/CellStoreV5.h>
#include <Hypertable/RangeServer/CellStoreV6.h>
#include <Hypertable/RangeServer/CellStoreV7.h>
#include <Hypertable/RangeServer/Global.h>

#include <Common/Filesystem.h>
#include <Common/Serialization.h>

#include <boost/shared_array.hpp>

using namespace Hypertable;
using namespace std;

CellStorePtr CellStoreFactory::open(const String &name,
                                    const char *start_row, const char *end_row) {
  String start = (start_row) ? start_row : "";
  String end = (end_row) ? end_row : Key::END_ROW_MARKER;
  int64_t file_length;
  int32_t fd;
  size_t nread, amount;
  uint64_t offset;
  uint16_t version;
  uint32_t oflags = 0;
  CellStorePtr cellstore;

  /** Get the file length **/
  file_length = Global::dfs->length(name, false);

  bool second_try = false;

  if (HT_IO_ALIGNED(file_length))
    oflags = Filesystem::OPEN_FLAG_DIRECTIO;

  /** Open the FS file **/
  fd = Global::dfs->open(name, oflags);

 try_again:

  amount = (file_length < HT_DIRECT_IO_ALIGNMENT) ? file_length : HT_DIRECT_IO_ALIGNMENT;
  offset = file_length - amount;

  boost::shared_array<uint8_t> trailer_buf( new uint8_t [amount] );

  nread = Global::dfs->pread(fd, trailer_buf.get(), amount, offset, second_try);

  if (nread != amount)
    HT_THROWF(Error::FSBROKER_IO_ERROR,
              "Problem reading trailer for CellStore file '%s'"
              " - only read %d of %lu bytes", name.c_str(),
              (int)nread, (Lu)amount);

  const uint8_t *ptr = trailer_buf.get() + (amount-2);
  size_t remaining = 2;

  version = Serialization::decode_i16(&ptr, &remaining);

  // If file format is < 4 and happens to be aligned, reopen non-directio
  if (version < 4 && oflags) {
    Global::dfs->close(fd);
    fd = Global::dfs->open(name, 0);
  }

  if (version == 7) {
    CellStoreTrailerV7 trailer_v7;

    if (amount < trailer_v7.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV7 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    try {
      trailer_v7.deserialize(trailer_buf.get() + (amount - trailer_v7.size()));
    }
    catch (Exception &e) {
      Global::dfs->close(fd);
      if (!second_try && e.code() == Error::CHECKSUM_MISMATCH) {
	fd = Global::dfs->open(name, oflags|Filesystem::OPEN_FLAG_VERIFY_CHECKSUM);
        second_try = true;
        goto try_again;
      }
      HT_ERRORF("Problem deserializing trailer of %s", name.c_str());
      throw;
    }

    cellstore = make_shared<CellStoreV7>(Global::dfs.get());
    cellstore->open(name, start, end, fd, file_length, &trailer_v7);
    if (!cellstore)
      HT_ERRORF("Failed to open CellStore %s [%s..%s], length=%llu",
              name.c_str(), start.c_str(), end.c_str(), (Llu)file_length);
    return cellstore;
  }
  else if (version == 6) {
    CellStoreTrailerV6 trailer_v6;

    if (amount < trailer_v6.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV6 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    try {
      trailer_v6.deserialize(trailer_buf.get() + (amount - trailer_v6.size()));
    }
    catch (Exception &e) {
      Global::dfs->close(fd);
      if (!second_try && e.code() == Error::CHECKSUM_MISMATCH) {
	fd = Global::dfs->open(name, oflags|Filesystem::OPEN_FLAG_VERIFY_CHECKSUM);
        second_try = true;
        goto try_again;
      }
      HT_ERRORF("Problem deserializing trailer of %s", name.c_str());
      throw;
    }

    cellstore = make_shared<CellStoreV6>(Global::dfs.get());
    cellstore->open(name, start, end, fd, file_length, &trailer_v6);
    if (!cellstore)
      HT_ERRORF("Failed to open CellStore %s [%s..%s], length=%llu",
              name.c_str(), start.c_str(), end.c_str(), (Llu)file_length);
    return cellstore;
  }
  else if (version == 5) {
    CellStoreTrailerV5 trailer_v5;

    if (amount < trailer_v5.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV5 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v5.deserialize(trailer_buf.get() + (amount - trailer_v5.size()));

    cellstore = make_shared<CellStoreV5>(Global::dfs.get());
    cellstore->open(name, start, end, fd, file_length, &trailer_v5);
    if (!cellstore)
      HT_ERRORF("Failed to open CellStore %s [%s..%s], length=%llu",
              name.c_str(), start.c_str(), end.c_str(), (Llu)file_length);
    return cellstore;
  }
  else if (version == 4) {
    CellStoreTrailerV4 trailer_v4;

    if (amount < trailer_v4.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV4 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v4.deserialize(trailer_buf.get() + (amount - trailer_v4.size()));

    cellstore = make_shared<CellStoreV4>(Global::dfs.get());
    cellstore->open(name, start, end, fd, file_length, &trailer_v4);
    if (!cellstore)
      HT_ERRORF("Failed to open CellStore %s [%s..%s], length=%llu",
              name.c_str(), start.c_str(), end.c_str(), (Llu)file_length);
    return cellstore;
  }
  else if (version == 3) {
    CellStoreTrailerV3 trailer_v3;

    if (amount < trailer_v3.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV3 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v3.deserialize(trailer_buf.get() + (amount - trailer_v3.size()));

    cellstore = make_shared<CellStoreV3>(Global::dfs.get());
    cellstore->open(name, start, end, fd, file_length, &trailer_v3);
    if (!cellstore)
      HT_ERRORF("Failed to open CellStore %s [%s..%s], length=%llu",
              name.c_str(), start.c_str(), end.c_str(), (Llu)file_length);
    return cellstore;
  }
  else if (version == 2) {
    CellStoreTrailerV2 trailer_v2;

    if (amount < trailer_v2.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV2 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v2.deserialize(trailer_buf.get() + (amount - trailer_v2.size()));

    cellstore = make_shared<CellStoreV2>(Global::dfs.get());
    cellstore->open(name, start, end, fd, file_length, &trailer_v2);
    if (!cellstore)
      HT_ERRORF("Failed to open CellStore %s [%s..%s], length=%llu",
              name.c_str(), start.c_str(), end.c_str(), (Llu)file_length);
    return cellstore;
  }
  else if (version == 1) {
    CellStoreTrailerV1 trailer_v1;

    if (amount < trailer_v1.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV1 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v1.deserialize(trailer_buf.get() + (amount - trailer_v1.size()));

    cellstore = make_shared<CellStoreV1>(Global::dfs.get());
    cellstore->open(name, start, end, fd, file_length, &trailer_v1);
    if (!cellstore)
      HT_ERRORF("Failed to open CellStore %s [%s..%s], length=%llu",
              name.c_str(), start.c_str(), end.c_str(), (Llu)file_length);
    return cellstore;
  }
  else if (version == 0) {
    CellStoreTrailerV0 trailer_v0;

    if (amount < trailer_v0.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV0 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v0.deserialize(trailer_buf.get() + (amount - trailer_v0.size()));

    cellstore = make_shared<CellStoreV0>(Global::dfs.get());
    cellstore->open(name, start, end, fd, file_length, &trailer_v0);
    if (!cellstore)
      HT_ERRORF("Failed to open CellStore %s [%s..%s], length=%llu",
              name.c_str(), start.c_str(), end.c_str(), (Llu)file_length);
    return cellstore;
  }
  else {
    Global::dfs->close(fd);
    if (!second_try) {
      fd = Global::dfs->open(name, oflags|Filesystem::OPEN_FLAG_VERIFY_CHECKSUM);
      second_try = true;
      goto try_again;
    }
    HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
	      "Unrecognized cell store version %d", (int)version);
  }
  return CellStorePtr();
}

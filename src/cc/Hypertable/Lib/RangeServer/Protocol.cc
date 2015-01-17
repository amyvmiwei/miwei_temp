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
 * Definitions for RangeServerProtocol.
 * This file contains definitions for RangeServerProtocol, a class for
 * generating RangeServer protocol messages.
 */

#include <Common/Compat.h>

#include "Protocol.h"

#include <Hypertable/Lib/RangeServerRecovery/ReceiverPlan.h>

#include <AsyncComm/CommBuf.h>
#include <AsyncComm/CommHeader.h>

using namespace Hypertable::Lib::RangeServer;
using namespace std;

string Protocol::compact_flags_to_string(uint32_t flags) {
  string str;
  bool first=true;
  if ((flags & COMPACT_FLAG_ALL) == COMPACT_FLAG_ALL) {
    str += "ALL";
    first=false;
  }
  else {
    if ((flags & COMPACT_FLAG_ROOT) == COMPACT_FLAG_ROOT) {
      str += "ROOT";
      first=false;
    }
    if ((flags & COMPACT_FLAG_METADATA) == COMPACT_FLAG_METADATA) {
      if (!first)
        str += "|";
      str += "METADATA";
      first=false;
    }
    if ((flags & COMPACT_FLAG_SYSTEM) == COMPACT_FLAG_SYSTEM) {
      if (!first)
        str += "|";
      str += "SYSTEM";
      first=false;
    }
    if ((flags & COMPACT_FLAG_USER) == COMPACT_FLAG_USER) {
      if (!first)
        str += "|";
      str += "USER";
      first=false;
    }
  }
  if ((flags & COMPACT_FLAG_MINOR) == COMPACT_FLAG_MINOR) {
    if (!first)
      str += "|";
    str += "MINOR";
    first=false;
  }
  if ((flags & COMPACT_FLAG_MAJOR) == COMPACT_FLAG_MAJOR) {
    if (!first)
      str += "|";
    str += "MAJOR";
    first=false;
  }
  if ((flags & COMPACT_FLAG_MERGING) == COMPACT_FLAG_MERGING) {
    if (!first)
      str += "|";
    str += "MERGING";
    first=false;
  }
  if ((flags & COMPACT_FLAG_GC) == COMPACT_FLAG_GC) {
    if (!first)
      str += "|";
    str += "GC";
    first=false;
  }
  return str;
}

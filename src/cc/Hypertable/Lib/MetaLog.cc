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

/** @file
 * Definitions for MetaLog.
 * This file contains definitions that are part of the MetaLog system.
 */

#include <Common/Compat.h>

#include "MetaLog.h"

#include <Common/Filesystem.h>
#include <Common/Serialization.h>

#include <algorithm>
#include <cctype>

using namespace Hypertable;
using namespace Hypertable::MetaLog;

void Header::encode(uint8_t **bufp) const {
  Serialization::encode_i16(bufp, version);
  memcpy(*bufp, name, 14);
  *bufp += 14;
}

void Header::decode(const uint8_t **bufp, size_t *remainp) {
  HT_ASSERT(*remainp >= LENGTH);
  version = Serialization::decode_i16(bufp, remainp);
  memcpy(name, *bufp, 14);
  *bufp += 14;
}


void MetaLog::scan_log_directory(FilesystemPtr &fs, const string &path,
                                 std::deque<int32_t> &file_ids) {
  const char *ptr;
  int32_t id;
  std::vector<Filesystem::Dirent> listing;

  if (!fs->exists(path))
    return;

  fs->readdir(path, listing);

  for (size_t i=0; i<listing.size(); i++) {

    for (ptr=listing[i].name.c_str(); ptr; ++ptr) {
      if (!isdigit(*ptr))
        break;
    }

    if (*ptr == 0 || (ptr > listing[i].name.c_str() && !strcmp(ptr, ".bad")))
      id = atoi(listing[i].name.c_str());
  
    if (*ptr != 0) {
      HT_WARNF("Invalid META LOG file name encountered '%s', skipping...",
               listing[i].name.c_str());
      continue;
    }

    file_ids.push_back(id);
  }

  // reverse sort
  sort(file_ids.begin(), file_ids.end(),
       [](int32_t lhs, int32_t rhs) { return lhs > rhs; });

}

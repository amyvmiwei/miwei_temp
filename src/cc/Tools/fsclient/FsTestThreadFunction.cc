/*
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
/// Definitions for FsTestThreadFunction.
/// This file contains definitions for FsTestThreadFunction, a thread function
/// class for running fsclient test operations.

#include <Common/Compat.h>

#include "FsTestThreadFunction.h"
#include "Utility.h"

#include <Common/Error.h>
#include <Common/Logger.h>

#include <cerrno>
#include <iostream>
#include <vector>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
}

using namespace Hypertable;
using namespace std;

void fsclient::FsTestThreadFunction::operator()() {

  try {

    copy_from_local(m_client, m_input_file, m_dfs_file);

    copy_to_local(m_client, m_dfs_file, m_output_file);

    // Determine original file size
    struct stat statbuf;
    if (stat(m_input_file.c_str(), &statbuf) != 0)
      HT_THROW(Error::EXTERNAL, (std::string)"Unable to stat file '"
               + m_input_file + "' - " + strerror(errno));

    int64_t origsz = statbuf.st_size;

    // Make sure file exists
    HT_ASSERT(m_client->exists(m_dfs_file));

    // Determine DFS file size
    int64_t dfssz1 = m_client->length(m_dfs_file, false);
    int64_t dfssz2 = m_client->length(m_dfs_file, true);

    if (origsz != dfssz1) {
      HT_ERRORF("Length mismatch: %lld != %lld", (Lld)origsz, (Lld)dfssz1);
      exit(1);
    }
    if (origsz != dfssz2) {
      HT_ERRORF("Length mismatch: %lld != %lld", (Lld)origsz, (Lld)dfssz2);
      exit(1);
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  /**
  cmd_rm.push_arg(m_dfs_file, "");
  if (cmd_rm.run() != 0)
    exit(1);
  **/
}

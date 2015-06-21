/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
/// Declarations for TransferLog
/// This file contains type declarations for TransferLog, a class for creating
/// a uniq range transfer log path

#ifndef Hypertable_RangeServer_TransferLog_h
#define Hypertable_RangeServer_TransferLog_h

#include <Common/Error.h>
#include <Common/Filesystem.h>
#include <Common/Logger.h>
#include <Common/md5.h>

#include <chrono>
#include <ctime>
#include <string>
#include <thread>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Creates a unique range transfer log pathname.
  class TransferLog {
  public:

    /// Constructor.
    /// Initializes #m_logname with a new transfer log path with the following
    /// format:
    /// <pre>
    /// &lt;toplevel-dir&gt;/tables/&lt;table-id&gt;/_xfer/&lt;end-row-hash&gt;_&lt;timestamp&gt;
    /// </pre>
    /// Where the &lt;end-row-hash&gt; component is the base-64 MD5 hash of
    /// <code>end_row</code> and &lt;timestamp&gt; is the current time in
    /// Epoch time.
    /// @param fs Pointer to filesystem
    /// @param toplevel_dir Toplevel database directory in fs
    /// @param table_id Table identifier of corresponding table
    /// @param end_row End row of corresponding range
    TransferLog(FilesystemPtr &fs, const string &toplevel_dir,
                const string &table_id, const string &end_row) {
      time_t now {};
      char md5DigestStr[33];
      md5_trunc_modified_base64(end_row.c_str(), md5DigestStr);
      md5DigestStr[16] = 0;
      do {
        if (now != 0)
          std::this_thread::sleep_for(std::chrono::milliseconds(1200));
        now = time(0);
        m_logname = format("%s/tables/%s/_xfer/%s_%d", toplevel_dir.c_str(),
                           table_id.c_str(), md5DigestStr, (int)now);
      }
      while (fs->exists(m_logname));
    }

    /// Returns pathname of transfer log.
    /// @return Name of transfer log.
    string &name() { return m_logname; }

  private:

    /// Full path name of transfer log
    string m_logname;

  };

  /// @}

}

#endif // Hypertable_RangeServer_TransferLog_h


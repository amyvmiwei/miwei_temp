/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
/// Declarations for Cronolog.
/// This file contains type declarations for Cronolog, a class that provides a
/// time based rotating log similar to that of cronolog (http://cronolog.org/)

#ifndef Common_Cronolog_h
#define Common_Cronolog_h

#include <ctime>
#include <mutex>
#include <string>

namespace Hypertable {

  using namespace std;

  /// @addtogroup Common
  /// @{

  /// Time based rotating log.
  /// This class provides a simple time based rotating log file similar to that
  /// of cronolog (http://cronolog.org/).  It maintains a log file and will roll
  /// it over at the end of the day.  It writes each daily log into a file
  /// relative to the archive directory with the following format:
  /// <pre>
  /// YYYY-MM/DD/<log-name>
  /// </pre>
  /// Where YYYY is the year, MM is the two-digit month, and DD is the two-digit
  /// day of the month.  It also sets up a symlink in the current directory
  /// to point to the current log archive file.  For example, if the name of the
  /// log is <code>SlowQuery.log</code> and the current directory is
  /// <code>/opt/hypertable/0.9.8.0/log</code> and the log archive directory is
  /// <code>/opt/hypertable/0.9.8.0/log/archive</code>, then a listing of the
  /// current directory on 2014-09-11 would show the following symlink:
  /// <pre>
  /// SlowQuery.log -> /opt/hypertable/0.9.8.0/log/archive/2014-09/11/SlowQuery.log
  /// </pre>
  class Cronolog {

  public:

    /// Constructor.
    /// @param name Basename of log file
    /// @param current_dir Directory in which the current symlink is created
    /// @param archive_dir Root archive directory
    Cronolog(const string &name, const string &current_dir, const string &archive_dir);

    /// Destructor.
    /// Closes the log file.
    virtual ~Cronolog();

    /// Writes a log line.
    /// This method first checks to see if the log needs to be rolled by
    /// comparing <code>now</code> with #m_roll_time and calls roll() if a roll
    /// is needed.  It then writes the log line as follows:
    /// <pre>
    /// now line\n
    /// </pre>
    /// To guarantee that the write makes it into the log, the object can be
    /// destroyed or the sync() method can be called.
    /// @param now Current time as seconds since Epoch
    /// @param line Log line to write
    void write(time_t now, const string &line);

    /// Syncs the log file.
    /// Calls fsync on the log file descriptor.
    void sync();

  private:

    /// Rolls the log.
    /// This method performs the following actions:
    ///   - Removes the current symlink if it exists
    ///   - Formulates the pathname of the new archive file, creating all parent
    ///     directories if they do not exist
    ///   - Opens the new log archive file
    ///   - Creates the current symlink to point to the new log archive file
    ///   - Sets #m_roll_time to the next time (seconds since Epoch) the log
    ///     needs to be rolled
    /// @param now Current time (seconds since Epoch)
    void roll(time_t now);

    /// %Mutex for serializing access to member variables
    mutex m_mutex;

    /// Basename of log file
    string m_name;

    /// Directory containing current symlink of log file
    string m_current_dir;

    /// Directory containing log archives
    string m_archive_dir;

    /// Time (seconds since Epoch) when next roll is required
    time_t m_roll_time;

    /// Log file descriptor
    int m_fd {};
    
  };

  /// @}
}

#endif // Common_Cronolog_h

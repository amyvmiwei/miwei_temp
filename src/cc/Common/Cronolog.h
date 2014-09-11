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
/// rotating log similar to that of cronolog (http://cronolog.org/)

#ifndef Common_Cronolog_h
#define Common_Cronolog_h

#include <ctime>
#include <mutex>

namespace Hypertable {

  using namespace std;

  /// @addtogroup Common
  /// @{

  /// Rotating log.
  class Cronolog {

  public:

    /// Constructor.
    Cronolog(const string &name, const string &current_dir, const string &archive_dir);

    /// Destructor.
    virtual ~Cronolog();

    void write(time_t now, const string &line);

    void sync();

  private:

    void roll(time_t now);

    // %Mutex for serializing access to member variables
    mutex m_mutex;

    // Basename of log file
    string m_name;

    // Directory containing current symlink of log file
    string m_current_dir;

    // Directory containing log archives
    string m_archive_dir;

    time_t m_roll_time;

    int m_fd {};
    
  };

  /// @}
}

#endif // Common_Cronolog_h

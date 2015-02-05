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
/// Declarations for StatusPersister.
/// This file contains declarations for StatusPersister, a class to hold Nagios-style
/// status information for a running program.

#ifndef Common_StatusPersister_h
#define Common_StatusPersister_h

#include <Common/Filesystem.h>
#include <Common/Status.h>

#include <mutex>
#include <string>
#include <vector>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Holds Nagios-style program status information.
  class StatusPersister {

  public:

    /// Sets persistent status.
    static void set(const Status &status,
                    std::vector<std::string> additional_lines);

    /// Gets persistent status.
    static void get(Status &status);

  private:

    static void initialize();

    /// %Mutex for serializaing access to members
    static std::mutex ms_mutex;

    /// %Mutex for serializaing access to members
    static std::string ms_fname;

    /// Current persistent status;
    static Status ms_status;

  };

  /// @}
}


#endif // Common_StatusPersister_h

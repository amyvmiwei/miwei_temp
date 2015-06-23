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
/// Declarations for StatusManager.
/// This file contains declarations for StatusManager, a class for managing the
/// status of a file system broker.

#ifndef FsBroker_Lib_StatusManager_h
#define FsBroker_Lib_StatusManager_h

#include <Common/Status.h>

#include <cerrno>
#include <chrono>
#include <mutex>

namespace Hypertable {
namespace FsBroker {
namespace Lib {

  /// @addtogroup FsBrokerLib
  /// @{

  /// Manages file system broker status
  class StatusManager {
  public:

    /// Constructor.
    StatusManager();

    /// Sets read status.
    /// @param code %Status code.
    /// @param text %Status text.
    void set_read_status(Status::Code code, const std::string &text);

    /// Sets write status.
    /// @param code %Status code.
    /// @param text %Status text.
    void set_write_status(Status::Code code, const std::string &text);

    /// Sets status to CRITICAL with status text associated with errno.
    /// @param error Error code (errno)
    void set_read_error(int error);

    /// Sets status to CRITICAL with status text associated with errno.
    /// @param error Error code (errno)
    void set_write_error(int error);

    /// Clears status by setting it to OK.
    void clear_status();

    /// Gets status information.
    /// @return Status information.
    Status &get() { return m_status; }

  private:

    /// Sets status.
    /// @param code %Status code.
    /// @param text %Status text.
    void set_status(Status::Code code, const std::string &text);

    /// Sets status to CRITICAL with status text associated with errno.
    /// @param error Error code (errno)
    void set_error(int error);

    /// %Mutex for serializaing access to members
    std::mutex m_mutex;

    /// Status information
    Status m_status;

    /// Time of last reported read error
    std::chrono::steady_clock::time_point m_last_read_error;

    /// Time of last reported write error
    std::chrono::steady_clock::time_point m_last_write_error;

    /// Current status
    Status::Code m_current_status {};
  };

  /// @}

}}}

#endif // FsBroker_Lib_Broker_h

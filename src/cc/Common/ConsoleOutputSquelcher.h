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
/// Declaration for ConsoleOutputSquelcher.
/// This file contains the declaration of ConsoleOutputSquelcher, a class for
/// temporarily redirecting <i>stdout</i> and <i>stderr</i> to
/// <code>/dev/null</code>.

#ifndef Hypertable_Common_ConsoleOutputSquelcher_h
#define Hypertable_Common_ConsoleOutputSquelcher_h

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Temporarily redirects <i>stdout</i> and <i>stderr</i> to
  /// <code>/dev/null</code>.
  class ConsoleOutputSquelcher {
  public:
    
    /// Constructor.
    /// Redirects <i>stdout</i> and <i>stderr</i> to platform equivalent of
    /// Unix <code>/dev/null</code>.
    ConsoleOutputSquelcher();

    /// Destructor.
    /// Restores <i>stdout</i> and <i>stderr</i>.
    ~ConsoleOutputSquelcher();

  private:

    /// Backup of stdout file descriptor
    int m_backup_stdout_fd {};

    /// Backup of stderr file descriptor
    int m_backup_stderr_fd {};
  };

  /// @}

}

#endif // Hypertable_Common_ConsoleOutputSquelcher_h

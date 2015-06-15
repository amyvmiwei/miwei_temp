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
/// Declarations for HqlCommandInterpreter.
/// This file contains type declarations for HqlCommandInterpreter, a class for
/// executing HQL commands with statistic written to stderr.

#ifndef Hypertable_Lib_HqlCommandInterpreter_h
#define Hypertable_Lib_HqlCommandInterpreter_h

#include <Hypertable/Lib/HqlInterpreter.h>

#include <Tools/Lib/CommandInterpreter.h>

namespace Hypertable {

  class Client;

  /// @addtogroup libHypertable
  /// @{

  /// HQL command interpreter.
  class HqlCommandInterpreter : public CommandInterpreter {
  public:

    /// Constructor.
    /// @param client Hypertable client object
    /// @param profile Flag indicating that scan queries should be profiled
    HqlCommandInterpreter(Client *client, bool profile=false);

    /// Constructor.
    HqlCommandInterpreter(HqlInterpreter *interp);

    /// Executes an HQL command.
    /// Executes an HQL command provided in <code>line</code>.  For write
    /// commands, a progress meter is used to report progress to stdout and
    /// statistics are reported to stderr when finished.  If constructed with
    /// the profile flag, then profile information is reported to stderr when
    /// SELECT queries are issued.
    /// @param line HQL command to execute
    /// @return Command return code
    int execute_line(const std::string &line) override;

  private:

    /// HQL interpreter
    HqlInterpreterPtr m_interp;

    /// Flag indicating if SELECT commands should be profiled
    bool m_profile {};
  };

  /// @}

}

#endif // Hypertable_Lib_HqlCommandInterpreter_h

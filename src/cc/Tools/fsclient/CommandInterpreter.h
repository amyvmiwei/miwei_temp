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
/// Declarations for CommandInterpreter.
/// This file contains declarations for CommandInterpreter, a class for
/// executing file system client commands.

#ifndef Tools_fsclient_CommandInterpreter_h
#define Tools_fsclient_CommandInterpreter_h

#include <FsBroker/Lib/Client.h>

#include <Tools/Lib/CommandInterpreter.h>

#include "AsyncComm/Comm.h"

#include <memory>
#include <string>
#include <unordered_map>

namespace Hypertable {
namespace fsclient {

  /// @addtogroup fsclient
  /// @{

  /// Command interpreter for %fsclient
  class CommandInterpreter : public Hypertable::CommandInterpreter {
  public:

    /// Constructor.
    /// Initializes #m_client with <code>client</code> and #m_nowait with
    /// <code>nowait</code>.  Also loads help text with a call to
    /// load_help_text().
    /// @param client File system client
    /// @param nowait No wait flag
    CommandInterpreter(FsBroker::Lib::ClientPtr &client, bool nowait=false)
      : m_client(client), m_nowait(nowait) {
      load_help_text();
    };

    int execute_line(const String &line) override;

  private:

    class ParseResult {
    public:
      void clear();
      int32_t command {};
      std::vector<std::string> args;
      int64_t offset {};
    };

    /// Parses a single command.
    /// @param line String containing command to parse
    /// @param result Reference to variable to hold parse result
    void parse_line(const String &line, ParseResult &result) const;

    /// Loads help text.
    /// Populates #m_help_text with help text for supported commands.
    void load_help_text();

    /// Displays help text for command.
    /// Searches #m_help_text for help text for <code>command</code> and
    /// displays it if found.
    /// @param command Command for which to display help text
    void display_help_text(const std::string &command) const;

    /// Displays usage for command.
    /// Searches #m_help_text for help text for <code>command</code> and
    /// displays the first line, which is the command usage.
    /// @param command Command for which to display help text
    void display_usage(const std::string &command) const;

    void parse_error(const std::string &command) const;

    /// File system client
    FsBroker::Lib::ClientPtr m_client;

    /// Map of help text
    std::unordered_map<std::string, const char **> m_help_text;

    /// Don't wait for certain commands to complete (e.g. shutdown)
    bool m_nowait {};
  };

  /// Smart pointer to CommandInterpreter
  typedef std::shared_ptr<CommandInterpreter> CommandInterpreterPtr;

  /// @}

}}

#endif // Tools_fsclient_CommandInterpreter_h

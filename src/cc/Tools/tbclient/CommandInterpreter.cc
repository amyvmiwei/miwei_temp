/*
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
/// Definitions for CommandInterpreter.
/// This file contains definitions for CommandInterpreter, a class for
/// executing thrift broker client commands.

#include <Common/Compat.h>

#include "CommandInterpreter.h"

#include <ThriftBroker/Client.h>

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Status.h>
#include <Common/ConsoleOutputSquelcher.h>

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <iostream>
#include <string>
#include <vector>

extern "C" {
#include <strings.h>
}

using namespace Hypertable;
using namespace std;

namespace {
  enum {
    COMMAND_NONE = 0,
    COMMAND_HELP,
    COMMAND_SHUTDOWN,
    COMMAND_STATUS
  };
}

int tbclient::CommandInterpreter::execute_line(const String &line) {
  ParseResult parse;

  parse_line(line, parse);

  switch (parse.command) {

  case COMMAND_NONE:
    break;

  case COMMAND_HELP:
    if (parse.args.empty())
      parse.args.push_back("contents");
    display_help_text(parse.args[0]);
    break;

  case COMMAND_SHUTDOWN:
    try {
      ConsoleOutputSquelcher temp;
      m_client->shutdown();
    }
    catch (ThriftGen::ClientException &e) {
    }
    catch (std::exception &e) {
    }
    break;

  case COMMAND_STATUS:
    {
      ThriftGen::Status status;
      {
        ConsoleOutputSquelcher temp;
        m_client->status(status);
      }
      if (!m_silent) {
        cout << "ThriftBroker "
             << Status::code_to_string(static_cast<Status::Code>(status.code));
        if (!status.text.empty())
          cout << " - " << status.text;
        cout << endl;
      }
      return status.code;
    }
    break;

  default:
    HT_FATALF("Unrecognized command - %d", (int)parse.command);
  }

  return 0;
}


void tbclient::CommandInterpreter::ParseResult::clear() {
  command = 0;
  args.clear();
  offset = 0;
}


void tbclient::CommandInterpreter::parse_line(const String &line, ParseResult &result) const {
  string command;

  result.clear();
  
  boost::char_separator<char> sep(" ");
  boost::tokenizer< boost::char_separator<char> > tokens(line, sep);
  for (const string& arg : tokens) {
    if (command.empty()) {
      command = arg;
    }
    else if (boost::algorithm::starts_with(arg, "--seek=")) {
      char *base = (char *)arg.c_str() + 7;
      char *end;
      errno = 0;
      result.offset = strtoll(base, &end, 10);
      if (errno != 0 || end == base) {
        parse_error(command);
        return;
      }
    }
    else
      result.args.push_back(arg);
  }

  if (command.empty())
    return;

  if (!strcasecmp(command.c_str(), "help")) {
    result.command = COMMAND_HELP;
  }
  else if (!strcasecmp(command.c_str(), "shutdown")) {
    if (result.args.empty() ||
        (result.args.size() == 1 && !strcasecmp(result.args[0].c_str(), "now")))
      result.command = COMMAND_SHUTDOWN;
    else
      parse_error(command);
  }
  else if (!strcasecmp(command.c_str(), "status")) {
    if (!result.args.empty())
      parse_error(command);
    else
      result.command = COMMAND_STATUS;
  }
  else {
    if (m_interactive)
      cout << "Unrecognized command - '" << command << "'" << endl;
    else
      HT_FATALF("Unrecognized command - '%s'", command.c_str());
  }

}


namespace {

  const char *g_help_text_contents[] = {
    "shutdown .............. Shutdown ThriftBroker",
    "status ................ Get status of ThriftBroker",
    "",
    "Statements must be terminated with ';'.  For more information on",
    "a specific statement, type 'help <statement>', where <statement> is from",
    "the preceeding list.",
    nullptr
  };

  const char *g_help_text_shutdown[] = {
    "shutdown [now]",
    "",
    "  This command sends a shutdown request to the filesystem broker.",
    "  If the 'now' argument is given, the broker will do an unclean",
    "  shutdown by exiting immediately.  Otherwise, it will wait for",
    "  all pending requests to complete before shutting down.",
    nullptr
  };

  const char *g_help_text_status[] = {
    "status",
    "",
    "  This command sends a status request to the filesystem broker, printing",
    "  the status output message to the console and returning the status code.",
    "  The return value of the last command issued to the interpreter will be",
    "  used as the exit status.",
    nullptr
  };
}

void tbclient::CommandInterpreter::load_help_text() {
  m_help_text["contents"] = g_help_text_contents;
  m_help_text["shutdown"] = g_help_text_shutdown;
  m_help_text["status"] = g_help_text_status;
}


void tbclient::CommandInterpreter::display_help_text(const std::string &command) const {
  string lower(command);
  boost::algorithm::to_lower(lower);
  auto iter = m_help_text.find(lower);
  if (iter != m_help_text.end()) {
    const char **text = iter->second;
    cout << endl;
    for (size_t i=0; text[i]; i++)
      cout << text[i] << endl;
    cout << endl;
  }
  else
    cout << endl << "No help for '" << command << "'" << endl << endl;
}

void tbclient::CommandInterpreter::display_usage(const std::string &command) const {
  string lower(command);
  boost::algorithm::to_lower(lower);
  auto iter = m_help_text.find(lower);
  HT_ASSERT(iter != m_help_text.end());
  const char **text = iter->second;
  cout << "Usage: " << text[0] << endl;
}

void tbclient::CommandInterpreter::parse_error(const std::string &command) const {
  string lower(command);
  boost::algorithm::to_lower(lower);
  auto iter = m_help_text.find(lower);
  HT_ASSERT(iter != m_help_text.end());
  const char **text = iter->second;
  if (m_interactive) {
    cout << "Parse error." << endl;
    cout << "Usage:  " << text[0] << endl;
    return;
  }
  else
    HT_THROWF(Error::COMMAND_PARSE_ERROR, "Usage:  %s", text[0]);
}

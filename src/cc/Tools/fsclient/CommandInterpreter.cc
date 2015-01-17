/*
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
/// Definitions for CommandInterpreter.
/// This file contains definitions for CommandInterpreter, a class for
/// executing file system client commands.

#include <Common/Compat.h>

#include "CommandInterpreter.h"
#include "Utility.h"

#include <Hypertable/Lib/HqlHelpText.h>
#include <Hypertable/Lib/HqlParser.h>

#include <Common/Error.h>

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
    COMMAND_COPYFROMLOCAL,
    COMMAND_COPYTOLOCAL,
    COMMAND_EXISTS,
    COMMAND_HELP,
    COMMAND_LENGTH,
    COMMAND_MKDIRS,
    COMMAND_REMOVE,
    COMMAND_RMDIR,
    COMMAND_SHUTDOWN,
    COMMAND_STATUS
  };
}

int fsclient::CommandInterpreter::execute_line(const String &line) {
  ParseResult parse;

  parse_line(line, parse);

  switch (parse.command) {

  case COMMAND_NONE:
    break;

  case COMMAND_COPYFROMLOCAL:
    fsclient::copy_from_local(m_client, parse.args[0], parse.args[1], parse.offset);
    break;

  case COMMAND_COPYTOLOCAL:
    fsclient::copy_to_local(m_client, parse.args[0], parse.args[1], parse.offset);
    break;

  case COMMAND_EXISTS:
    cout << (m_client->exists(parse.args[0]) ? "true" : "false") << endl;
    break;

  case COMMAND_HELP:
    if (parse.args.empty())
      parse.args.push_back("contents");
    display_help_text(parse.args[0]);
    break;

  case COMMAND_LENGTH:
    cout << m_client->length(parse.args[0]) << endl;
    break;

  case COMMAND_MKDIRS:
    m_client->mkdirs(parse.args[0]);
    break;

  case COMMAND_REMOVE:
    m_client->remove(parse.args[0]);
    break;

  case COMMAND_RMDIR:
    m_client->rmdir(parse.args[0]);
    break;

  case COMMAND_SHUTDOWN:
    {
      uint16_t flags = 0;
      if (!parse.args.empty()) {
        HT_ASSERT(!strcasecmp(parse.args[0].c_str(), "now"));
        flags |= FsBroker::Lib::Client::SHUTDOWN_FLAG_IMMEDIATE;
      }
      if (m_nowait)
        m_client->shutdown(flags, 0);
      else {
        EventPtr event;
        DispatchHandlerSynchronizer sync_handler;
        m_client->shutdown(flags, &sync_handler);
        sync_handler.wait_for_reply(event);
      }
    }
    break;

  case COMMAND_STATUS:
    {
      string output;
      int32_t code = m_client->status(output);
      if (!m_silent)
        cout << "FsBroker " << output << endl;
      return code;
    }

  default:
    HT_FATALF("Unrecognized command - %d", (int)parse.command);
  }

  return 0;
}


void fsclient::CommandInterpreter::ParseResult::clear() {
  command = 0;
  args.clear();
  offset = 0;
}


void fsclient::CommandInterpreter::parse_line(const String &line, ParseResult &result) const {
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

  if (!strcasecmp(command.c_str(), "copyFromLocal")) {
    if (result.args.size() != 2)
      parse_error(command);
    else
      result.command = COMMAND_COPYFROMLOCAL;
  }
  else if (!strcasecmp(command.c_str(), "copyToLocal")) {
    if (result.args.size() != 2)
      parse_error(command);
    else
      result.command = COMMAND_COPYTOLOCAL;
  }
  else if (!strcasecmp(command.c_str(), "exists")) {
    if (result.args.size() != 1)
      parse_error(command);
    else
      result.command = COMMAND_EXISTS;
  }
  else if (!strcasecmp(command.c_str(), "help")) {
    result.command = COMMAND_HELP;
  }
  else if (!strcasecmp(command.c_str(), "length")) {
    if (result.args.size() != 1)
      parse_error(command);
    else
      result.command = COMMAND_LENGTH;
  }
  else if (!strcasecmp(command.c_str(), "mkdirs")) {
    if (result.args.size() != 1)
      parse_error(command);
    else
      result.command = COMMAND_MKDIRS;
  }
  else if (!strcasecmp(command.c_str(), "rm")) {
    if (result.args.size() != 1)
      parse_error(command);
    else
      result.command = COMMAND_REMOVE;
  }
  else if (!strcasecmp(command.c_str(), "rmdir")) {
    if (result.args.size() != 1)
      parse_error(command);
    else
      result.command = COMMAND_RMDIR;
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
    "copyFromLocal ......... Copy file from local filesystem to brokered filesystem",
    "copyToLocal ........... Copy file from brokered filesystem to local filesystem",
    "exists ................ Check for file existence",
    "length ................ Get length of file",
    "mkdirs ................ Create directory and missing parent directories",
    "rm .................... Remove file",
    "rmdir ................. Remove directory",
    "shutdown .............. Shutdown file system broker",
    "status ................ Get status of file system broker",
    "",
    "Statements must be terminated with ';'.  For more information on",
    "a specific statement, type 'help <statement>', where <statement> is from",
    "the preceeding list.",
    nullptr
  };

  const char *g_help_text_copyfromlocal[] = {
    "copyFromLocal [--seek=<offset>] <src> <dst>",
    "",
    "  This command copies the local file <src> to the remote file <dst>",
    "  in the brokered file system.  If --seek is supplied, then the source",
    "  file is seeked to offset <offset> before starting the copy.",
    nullptr
  };

  const char *g_help_text_copytolocal[] = {
    "copyToLocal [--seek=<offset>] <src> <dst>",
    "",
    "  This command copies the file <src> from the brokered file system to"
    "  the <dst> file in the local filesystem.  If --seek is supplied, then",
    "  the source file is seeked to offset <offset> before starting the copy.",
    nullptr
  };

  const char *g_help_text_exists[] = {
    "exists <file>",
    "",
    "  This command checks for the existence of <file> in the brokered",
    "  filesystem and prints \"true\" if it exists, or \"false\" otherwise.",
    nullptr
  };

  const char *g_help_text_length[] = {
    "length <file>",
    "",
    "  This command fetches the length of <file> in the brokered filesystem",
    "  and prints it to the console.",
    nullptr
  };

  const char *g_help_text_mkdirs[] = {
    "mkdirs <dir>",
    "",
    "  This command creates a directory and all of its missing parents in",
    "  the broked filesystem.",
    nullptr
  };

  const char *g_help_text_remove[] = {
    "rm <file>",
    "",
    "  This command removes the file <file> in the brokered filesystem.",
    nullptr
  };

  const char *g_help_text_rmdir[] = {
    "rmdir <dir>",
    "",
    "  This command recursively removes <dir> from the brokered filesystem",
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

void fsclient::CommandInterpreter::load_help_text() {
  m_help_text["contents"] = g_help_text_contents;
  m_help_text["copyfromlocal"] = g_help_text_copyfromlocal;
  m_help_text["copyToLocal"] = g_help_text_copytolocal;
  m_help_text["exists"] = g_help_text_exists;
  m_help_text["length"] = g_help_text_length;
  m_help_text["mkdirs"] = g_help_text_mkdirs;
  m_help_text["rm"] = g_help_text_remove;
  m_help_text["rmdir"] = g_help_text_rmdir;
  m_help_text["shutdown"] = g_help_text_shutdown;
  m_help_text["status"] = g_help_text_status;
}


void fsclient::CommandInterpreter::display_help_text(const std::string &command) const {
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

void fsclient::CommandInterpreter::display_usage(const std::string &command) const {
  string lower(command);
  boost::algorithm::to_lower(lower);
  auto iter = m_help_text.find(lower);
  HT_ASSERT(iter != m_help_text.end());
  const char **text = iter->second;
  cout << "Usage: " << text[0] << endl;
}

void fsclient::CommandInterpreter::parse_error(const std::string &command) const {
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

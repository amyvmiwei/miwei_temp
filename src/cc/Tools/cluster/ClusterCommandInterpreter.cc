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
/// Definitions for ClusterCommandInterpreter.
/// This file contains type definitions for ClusterCommandInterpreter, a class
/// for handling interactive command execution for the cluster tool.

#include <Common/Compat.h>

#include "ClusterCommandInterpreter.h"
#include "ClusterDefinition/TokenizerTools.h"

#include <Common/String.h>
#include <Common/System.h>

#include <boost/algorithm/string.hpp>

#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <vector>

extern "C" {
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
}

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace std;

namespace {

  bool split_command_line(const string &line, vector<string> &argv) {
    const char *base = line.c_str();
    while (*base) {
      while (*base && isspace(*base))
        base++;
      if (*base == 0)
        break;
      const char *ptr = base;
      if (*ptr == '"' || *ptr == '\'') {
        const char *end;
        if (!TokenizerTools::find_end_char(ptr, &end)) {
          cout << "Invalid command line - missing terminating quote" << endl;
          return false;
        }
        ptr++;
        argv.push_back(string(ptr, (end-ptr)-1));
        base = end+1;
      }
      else {
        while (*ptr && !isspace(*ptr))
          ptr++;
        argv.push_back(string(base, ptr-base));
        base = ptr;
      }
    }
    return true;
  }

  bool extract_command_line_argument(const char *base, const char **end, string &arg) {
    const char *ptr = base;
    while (*ptr && isspace(*ptr))
      ptr++;
    if (*ptr == '"' || *ptr == '\'') {
      if (!TokenizerTools::find_end_char(ptr, end)) {
        cout << "Invalid command line - missing terminating quote" << endl;
        return false;
      }
      arg.append(ptr, (*end)-ptr);
      (*end)++;
      boost::trim(arg);
    }
    else {
      base = ptr++;
      while (*ptr && !isspace(*ptr))
        ptr++;
      arg.append(base, ptr-base);
      *end = ptr;
      boost::trim(arg);
    }
    return true;
  }

  const char *help_text[] = {
    "",
    "Shell commands or cluster definition tasks can be run remotely on any",
    "set of machines using the following syntax.  Type 'quit' or 'exit' to",
    "exit the command interpreter.",
    "",
    "<command> [ <arg> ... ]",
    "",
    "  This instructs the interpreter to run the shell command (<command>)",
    "  with optional arguments (<arg> ...) on all hosts specified in all",
    "  the roles in the cluster definition file.",
    "",
    "  cluster> echo \"Hello, World!\"",
    "",
    "on <hostspec> <command> [ <arg> ... ]",
    "",
    "  This instructs the interpreter to run the shell command (<command>)",
    "  with optional arguments (<arg> ...) on all hosts matching a host",
    "  specification (<hostspec>).  The host specification is a compact",
    "  way to specify a large number of host names matching a pattern,",
    "  for example the host specification \"test[01-06] - test02\" expands",
    "  to:  test01, test03, test04, test05, test06.",
    "",
    "  cluster> on test[02-04]-test03 echo \"Hello, World!\"",
    "",
    "with <roles> <command> [ <arg> ... ]",
    "",
    "  This instructs the interpreter to run the shell command (<command>)",
    "  with optional arguments (<arg> ...) on all hosts represented by the",
    "  comma-separated list of role names (<roles>) specified in the cluster",
    "  definition file.",
    "",
    "  cluster> with master,slave echo \"Hello, World!\"",
    "",
    "!<task> [ on <hostspec> ] [<arg> ...]",
    "",
    "  This instructs the interpreter to run a task (<task>) with optional",
    "  arguments (<arg> ...) using the default roles supplied in the task",
    "  definition or on a specific set of hosts that match a host",
    "  specification pattern by supplying the \"on <hostspec>\" after the",
    "  task name.",
    "",
    "  cluster> !start_hyperspace",
    "  cluster> !start_hyperspace on test02",
    "",
    nullptr
  };

}


ClusterCommandInterpreter::ClusterCommandInterpreter(const string &script) 
  : m_command_script(script) {
  return;
}


int ClusterCommandInterpreter::execute_line(const String &line) {
  char **argv;
  const char *end;
  string program;
  string target;
  string command;
  string trimmed_line(line);
  boost::trim(trimmed_line);

  if (trimmed_line[0] == '!') {
    vector<string> args;
    if (!split_command_line(trimmed_line.substr(1), args) || args.empty())
      return 2;
    program = m_command_script;
    argv = new char *[args.size()+2];
    size_t i=0;
    argv[i++] = (char *)program.c_str();
    for (auto & arg : args)
      argv[i++] = (char *)arg.c_str();
    argv[i] = 0;
  }
  else if (!strcmp(trimmed_line.c_str(), "help")) {
    for (size_t i=0; help_text[i]; i++)
      cout << help_text[i] << "\n";
    cout << flush;
    return 0;
  }
  else if (!strncmp(trimmed_line.c_str(), "on ", 3)) {

    if (!extract_command_line_argument(trimmed_line.c_str() + 3, &end, target))
      return 2;

    command.append(end);
    if (command.empty()) {
      cout << "Invalid command line" << endl;
      return 2;
    }

    boost::trim_if(target, boost::is_any_of("'\""));

    program.append(System::install_dir.c_str());
    program.append("/bin/ht_ssh");

    argv = new char *[4];
    argv[0] = (char *)program.c_str();
    argv[1] = (char *)target.c_str();
    argv[2] = (char *)command.c_str();
    argv[3] = nullptr;

  }
  else {

    if (!strncmp(trimmed_line.c_str(), "with ", 5)) {
      if (!extract_command_line_argument(trimmed_line.c_str()+5, &end, target))
        return 2;
      command.append(end);
    }
    else {
      target = "all";
      command.append(trimmed_line);
    }

    if (command.empty()) {
      cout << "Invalid command line" << endl;
      return 2;
    }

    boost::trim_if(target, boost::is_any_of("'\""));

    program = m_command_script;
    argv = new char *[5];
    argv[0] = (char *)program.c_str();
    argv[1] = (char *)"with";
    argv[2] = (char *)target.c_str();
    argv[3] = (char *)command.c_str();
    argv[4] = nullptr;
  }

  pid_t pid = vfork();

  if (pid == 0) {
    if (execv(program.c_str(), argv) < 0) {
      cout << "execv(" << program << ") failed - " << strerror(errno) << endl;
      quick_exit(2);
    }
  }
  else if (pid < 0) {
    cout << "vfork() failed - " << strerror(errno) << endl;
    quick_exit(2);
  }
  else {
    int status;
    waitpid(pid, &status, 0);
    delete [] argv;
    return status;
  }
  return 0;
}


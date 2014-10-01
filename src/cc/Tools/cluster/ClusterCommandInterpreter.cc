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

extern "C" {
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
}

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace std;

namespace {

  const string escape_string(const string &str) {
    string escaped_str;
    const char *ptr;
    for (ptr = str.c_str(); *ptr; ptr++) {
      if (isspace(*ptr))
        break;
    }
    if (*ptr == 0)
      return str;
    escaped_str.reserve(str.length()*2);
    escaped_str.append(1, '"');
    for (ptr = str.c_str(); *ptr; ptr++) {
      if (*ptr == '"') {
        if (ptr == str.c_str() || *(ptr-1) != '\\')
          escaped_str.append(1, '\\');
        escaped_str.append(1, '"');
      }
      else
        escaped_str.append(1, *ptr);
    }
    escaped_str.append(1, '"');
    return escaped_str;
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


}


ClusterCommandInterpreter::ClusterCommandInterpreter(const string &script) 
  : m_command_script(script) {
  return;
}


void ClusterCommandInterpreter::execute_line(const String &line) {
  char *argv[5];
  const char *end;
  string target;
  string command;
  string trimmed_line(line);
  boost::trim(trimmed_line);

  if (!strncmp(trimmed_line.c_str(), "on ", 3)) {

    if (!extract_command_line_argument(trimmed_line.c_str() + 3, &end, target))
      return;

    command.append(end);
    if (command.empty()) {
      cout << "Invalid command line" << endl;
      return;
    }

    boost::trim_if(target, boost::is_any_of("'\""));

    string ssh_binary;
    ssh_binary.append(System::install_dir.c_str());
    ssh_binary.append("/bin/ht_ssh");

    argv[0] = (char *)ssh_binary.c_str();
    argv[1] = (char *)target.c_str();
    argv[2] = (char *)command.c_str();
    argv[3] = nullptr;

    pid_t pid = vfork();

    if (pid == 0) {
      if (execv(ssh_binary.c_str(), argv) < 0) {
        cout << "execv() failed - " << strerror(errno) << endl;
        _exit(1);
      }
    }
    else if (pid < 0) {
      cout << "vfork() failed - " << strerror(errno) << endl;
      _exit(1);
    }
    else {
      int status;
      waitpid(pid, &status, 0);
    }
  }
  else {

    if (!strncmp(trimmed_line.c_str(), "with ", 5)) {
      if (!extract_command_line_argument(trimmed_line.c_str()+5, &end, target))
        return;
      command.append(end);
    }
    else {
      target = "all";
      command.append(trimmed_line);
    }

    if (command.empty()) {
      cout << "Invalid command line" << endl;
      return;
    }

    boost::trim_if(target, boost::is_any_of("'\""));

    argv[0] = (char *)m_command_script.c_str();
    argv[1] = (char *)"with";
    argv[2] = (char *)target.c_str();
    argv[3] = (char *)command.c_str();
    argv[4] = nullptr;

    pid_t pid = vfork();

    if (pid == 0) {
      if (execv(m_command_script.c_str(), argv) < 0) {
        cout << "execv() failed - " << strerror(errno) << endl;
        _exit(1);
      }
    }
    else if (pid < 0) {
      cout << "vfork() failed - " << strerror(errno) << endl;
      _exit(1);
    }
    else {
      int status;
      waitpid(pid, &status, 0);
    }
    
  }
}


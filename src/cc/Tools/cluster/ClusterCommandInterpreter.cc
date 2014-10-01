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
#include <cstdlib>
#include <iostream>

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

}


ClusterCommandInterpreter::ClusterCommandInterpreter() {
  return;
}


void ClusterCommandInterpreter::execute_line(const String &line) {
  string host_spec;
  string command;
  string trimmed_line(line);
  boost::trim(trimmed_line);
  if (!strncmp(trimmed_line.c_str(), "on ", 3)) {
    const char *ptr = trimmed_line.c_str() + 3;
    const char *end;
    while (*ptr && isspace(*ptr))
      ptr++;
    if (*ptr == '"' || *ptr == '\'') {
      if (!TokenizerTools::find_end_char(ptr, &end)) {
        cout << "Invalid command line - missing terminating quote" << endl;
        return;
      }
      ptr++;
      host_spec.append(ptr, (end-ptr)-1);
      command.append(end+1);
      boost::trim(command);
    }
    else {
      end = ptr+1;
      while (*end && !isspace(*end))
        end++;
      host_spec.append(ptr, end-ptr);
      command.append(end);
      boost::trim(command);
    }

    if (command.empty()) {
      cout << "Invalid command line" << endl;
      return;
    }

    command = escape_string(command);

    string ssh_command = format("%s/bin/ht_ssh \"%s\" %s",
                                System::install_dir.c_str(), host_spec.c_str(),
                                command.c_str());
    int ret = system(ssh_command.c_str());
    if (ret)
      cout << "Command exited with status " << ret << endl;
  }
  else {
    cout << "executing line: " << line << endl;
    cout << "executing line: ";
    for (const char *ptr = line.c_str(); *ptr; ptr++)
      cout << *ptr;
    cout << endl;
  }
}


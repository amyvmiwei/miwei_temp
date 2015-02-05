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

#include <Common/Compat.h>

#include "StatusPersister.h"

#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/System.h>

#include <cstdio>
#include <cstring>
#include <ctime>

extern "C" {
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
}

using namespace Hypertable;
using namespace std;

std::mutex StatusPersister::ms_mutex;
std::string StatusPersister::ms_fname;
Status StatusPersister::ms_status;

namespace {
  const char *header = 
    "# Persistent Status Log for %s\n"
    "# The last entry in this file will be returned during the %s\n"
    "# status check.  Once the issues described below have been dealt with, this file\n"
    "# can be removed to clear the %s persistent status and\n"
    "# return to normal dynamic status checking.\n"
    "#\n";
  string header_string() {
    return format(header, System::exe_name.c_str(), System::exe_name.c_str(),
                  System::exe_name.c_str());
  }
}

void StatusPersister::set(const Status &status,
                          std::vector<std::string> additional_lines) {
  lock_guard<mutex> lock(ms_mutex);

  if (ms_fname.empty())
    initialize();

  bool append {true};
  string text;

  if (!FileUtils::exists(ms_fname)) {
    text = header_string();
    append = false;
  }

  ms_status = status;

  Status::Code code;
  string status_text;
  ms_status.get(&code, status_text);
  text += format("%s - %s\n", Status::code_to_string(code), status_text.c_str());

  {
    time_t rawtime;
    tm* timeinfo;
    char buffer [80];
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", timeinfo);
    text += format("- Time %s\n", (const char *)buffer);
  }

  for (auto & str : additional_lines) {
    if (!str.empty())
      text += format("- %s\n", str.c_str());
  }

  if (append) {
    int fd = ::open(ms_fname.c_str(), O_WRONLY | O_APPEND);
    if (fd < 0) {
      string message = strerror(errno);
      HT_ERRORF("Unable to open file \"%s\" for writing - %s", ms_fname.c_str(),
                message.c_str());
    }
    else {
      FileUtils::write(fd, text);
      ::close(fd);
    }
  }
  else
    FileUtils::write(ms_fname, text);
}

void StatusPersister::get(Status &status) {
  lock_guard<mutex> lock(ms_mutex);
  if (ms_fname.empty())
    initialize();
  status = ms_status;
}

void StatusPersister::initialize() {
  ms_fname = System::install_dir + "/run/STATUS." + System::exe_name;
  if (FileUtils::exists(ms_fname)) {
    string last_line, contents;
    FileUtils::read(ms_fname, contents);
    const char *base = contents.c_str();
    const char *ptr = strchr(base, '\n');
    while (ptr) {
      if (*base != '#' && *base != '-')
        last_line = string(base, ptr-base);
      base = ptr+1;
      ptr = strchr(base, '\n');
    }
    if (*base && *base != '#' && *base != '-')
      last_line = string(base, ptr-base);
    if (!last_line.empty()) {
      if (last_line.find("WARNING - ") == 0)
        ms_status.set(Status::Code::WARNING, last_line.substr(10));
      else if (last_line.find("CRITICAL - ") == 0)
        ms_status.set(Status::Code::CRITICAL, last_line.substr(11));
      else if (last_line.find("UNKNOWN - ") == 0)
        ms_status.set(Status::Code::UNKNOWN, last_line.substr(10));
      else if (last_line.find("OK - ") != 0)
        HT_INFOF("Corrupt persistent status file %s", ms_fname.c_str());
    }
  }
}

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
/// Definitions for Cronolog.
/// This file contains type definitions for Cronolog, a class that provides a
/// time based rotating log similar to that of cronolog (http://cronolog.org/)

#include <Common/Compat.h>

#include "Cronolog.h"

#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/String.h>

#include <boost/algorithm/string.hpp>

#include <cerrno>
#include <cstdio>

extern "C" {
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
}

using namespace Hypertable;

Cronolog::Cronolog(const string &name, const string &current_dir,
                   const string &archive_dir) 
  : m_name(name), m_current_dir(current_dir), m_archive_dir(archive_dir) {
  boost::trim_right_if(m_current_dir, boost::is_any_of("/"));
  boost::trim_right_if(m_archive_dir, boost::is_any_of("/"));
  roll(time(0));
}

Cronolog::~Cronolog() {
  lock_guard<mutex> lock(m_mutex);
  if (m_fd) {
    ::close(m_fd);
    m_fd = 0;
  }
}

void Cronolog::write(time_t now, const string &line) {
  lock_guard<mutex> lock(m_mutex);

  if (!m_fd)
    return;

  if (now >= m_roll_time)
    roll(now);

  char buf[32];
  sprintf(buf, "%d ", (int)now);
  if (FileUtils::write(m_fd, buf, strlen(buf)) < 0 ||
      FileUtils::write(m_fd, line) < 0 ||
      FileUtils::write(m_fd, "\n", 1) < 0)
    HT_FATALF("write to log file '%s' failed - %s",
              m_name.c_str(), strerror(errno));

}

void Cronolog::sync() {
  lock_guard<mutex> lock(m_mutex);
  if (m_fd)
    ::fsync(m_fd);
}

void Cronolog::roll(time_t now) {

  if (m_fd)
    ::close(m_fd);

  string linkname = m_current_dir + "/" + m_name;

  if (FileUtils::exists(linkname)) {
    if (!FileUtils::unlink(linkname))
      HT_FATALF("Problem removing link '%s'", linkname.c_str());
  }

  struct tm tmval;
  
  localtime_r(&now, &tmval);

  string subdir = format("%s/%d-%02d", m_archive_dir.c_str(),
                         1900 + tmval.tm_year, tmval.tm_mon + 1);

  if (!FileUtils::exists(subdir)) {
    if (!FileUtils::mkdirs(subdir))
      HT_FATALF("Problem creating archive directory '%s'", subdir.c_str());
  }

  subdir += format("/%02d", tmval.tm_mday);

  if (!FileUtils::exists(subdir)) {
    if (!FileUtils::mkdirs(subdir))
      HT_FATALF("Problem creating archive directory '%s'", subdir.c_str());
  }

  string archive = format("%s/%s", subdir.c_str(), m_name.c_str());

  m_fd = ::open(archive.c_str(), O_CREAT|O_APPEND|O_WRONLY, 0644);
  if (m_fd < 0)
    HT_FATALF("open(%s, O_CREAT|O_APPEND|O_WRONLY, 0644) failed - %s",
              archive.c_str(), strerror(errno));

  {
    char cwd[1024];

    if (getcwd(cwd, 1024) == nullptr)
      HT_FATALF("getcwd failure - %s", strerror(errno));

    if (chdir(m_current_dir.c_str()) < 0)
      HT_FATALF("chdir(%s) failure - %s", m_current_dir.c_str(), strerror(errno));

    ::unlink(m_name.c_str());

    if (symlink(archive.c_str(), m_name.c_str()) < 0)
      HT_FATALF("symlink(%s, %s) failure - %s", archive.c_str(), m_name.c_str(),
                strerror(errno));

    if (chdir(cwd) < 0)
      HT_FATALF("chdir(%s) failure - %s", cwd, strerror(errno));
  }

  tmval.tm_sec = 0;
  tmval.tm_min = 0;
  tmval.tm_hour = 0;

  m_roll_time = mktime(&tmval) + 86400;

}

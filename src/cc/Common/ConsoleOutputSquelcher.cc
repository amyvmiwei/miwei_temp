/*
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
/// Definition for ConsoleOutputSquelcher.
/// This file contains the definition of ConsoleOutputSquelcher, a class for
/// temporarily redirecting <i>stdout</i> and </i>stderr</i> to
/// <code>/dev/null</code>.

#include <Common/Compat.h>

#include "ConsoleOutputSquelcher.h"

extern "C" {
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
}

#include <cstdio>

using namespace Hypertable;

ConsoleOutputSquelcher::ConsoleOutputSquelcher() {
#if defined(__WIN32__) || defined(_WIN32)
  const char *devnull = "nul";
#else
  const char *devnull = "/dev/null";
#endif
  int fd;
  // Redirect stdout
  fflush(stdout);
  m_backup_stdout_fd = dup(1);
  fd = ::open(devnull, O_WRONLY);
  dup2(fd, 1);
  close(fd);
  // Redirect stderr
  fflush(stderr);
  m_backup_stderr_fd = dup(2);
  fd = ::open(devnull, O_WRONLY);
  dup2(fd, 2);
  close(fd);
}

ConsoleOutputSquelcher::~ConsoleOutputSquelcher() {
  // Restore stdout
  fflush(stdout);
  dup2(m_backup_stdout_fd, 1);
  close(m_backup_stdout_fd);
  // Restore stderr
  fflush(stderr);
  dup2(m_backup_stderr_fd, 2);
  close(m_backup_stderr_fd);
}

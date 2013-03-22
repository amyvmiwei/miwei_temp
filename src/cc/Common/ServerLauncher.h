/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

/** @file
 * Launches an external command, redirects its output to a file. Used for
 * testing.
 */

#ifndef HYPERTABLE_SERVERLAUNCHER_H
#define HYPERTABLE_SERVERLAUNCHER_H

#include <iostream>

extern "C" {
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
}

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * Launches external commands and redirects their output to a file; kills
   * the external process when going out of scope.
   * This class is used for testing purposes.
   */
  class ServerLauncher {
  public:
    /** Constructor; launches the external command, optionally redirects the
     * output.
     *
     * @param path The path of the external program
     * @param argv The argument list of the program
     * @param outfile Path of the redirected output file (for stdout AND
     *      stderr); optional. If NULL then will use stdout/stderr of the
     *      host program
     * @param append_output If true, output will be appended. Otherwise output
     *      file will be overwritten
     */
    ServerLauncher(const char *path, char *const argv[],
            const char *outfile = 0, bool append_output = false) {
      int fd[2];
      m_path = path;
      if (pipe(fd) < 0) {
        perror("pipe");
        exit(1);
      }
      if ((m_child_pid = fork()) == 0) {
        if (outfile) {
          int open_flags;
          int outfd = -1;

          if (append_output)
            open_flags = O_CREAT | O_APPEND | O_RDWR;
          else
            open_flags = O_CREAT | O_TRUNC | O_WRONLY,

          outfd = open(outfile, open_flags, 0644);
          if (outfd < 0) {
            perror("open");
            exit(1);
          }
          dup2(outfd, 1);
          dup2(outfd, 2);
        }
        close(fd[1]);
        dup2(fd[0], 0);
        close(fd[0]);
        execv(path, argv);
      }
      close(fd[0]);
      m_write_fd = fd[1];
      poll(0, 0, 2000);
    }

    /** Destructor; kills the external program */
    ~ServerLauncher() {
      std::cerr << "Killing '" << m_path << "' pid=" << m_child_pid
          << std::endl << std::flush;
      close(m_write_fd);
      if (kill(m_child_pid, 9) == -1)
        perror("kill");
    }

    /** Returns stdin of the external program */
    int get_write_descriptor() const {
      return m_write_fd;
    }

    /** Returns the pid of the external program */
    pid_t get_pid() const {
      return m_child_pid;
    }

  private:
    /** The path of the external program */
    const char *m_path;

    /** The pid of the external program */
    pid_t m_child_pid;

    /** The file descriptor writing to stdin of the external program */
    int m_write_fd;
  };

  /** @} */

}

#endif // HYPERTABLE_SERVERLAUNCHER_H

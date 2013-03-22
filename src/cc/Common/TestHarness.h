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
 * A simple testing framework with some helpers for dealing with golden files
 * (comparing files, generating them etc.)
 */

#ifndef HYPERTABLE_TESTHARNESS_H
#define HYPERTABLE_TESTHARNESS_H

#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
}

#include "Logger.h"


namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * A simple test framework which sets up the logging subsystem, can compare
   * its output against a golden file or regenerate the golden file.
   */
  class TestHarness {
  public:
    /** Constructor; redirects the logging output to a file.
     *
     * @param name The application name; used in the logger's output and as
     *      part of the log filename
     */
    TestHarness(const char *name)
        : m_error(0) {
      Logger::initialize(name);

      // open temporary output file
      sprintf(m_output_file, "%s%d", name, (int)getpid());

      if ((m_fd = open(m_output_file, O_CREAT | O_TRUNC | O_WRONLY, 0644))
          < 0) {
        HT_ERRORF("open(%s) failed - %s", m_output_file, strerror(errno));
        exit(1);
      }

      Logger::get()->set_test_mode(m_fd);
    }

    /** Destructor; if the test was successful then the logfile is deleted */
    ~TestHarness() {
      if (!m_error)
        unlink(m_output_file);
    }

    /** Returns the file descriptor of the log file */
    int get_log_file_descriptor() { return m_fd; }

    /** Validates the log file output with the golden file, then exits. The
     * exit code depends on the golden file comparison - 0 on success, else for
     * error.
     *
     * @param golden_file Filename of the golden file
     */
    void validate_and_exit(const char *golden_file) {
      validate(golden_file);
      _exit(m_error ? 1 : 0);
    }

    /** Validates the log file against the golden file. Returns 0 on success,
     * else for failure. Uses `diff` to do the comparison.
     *
     * @param golden_file Filename of the golden file
     */
    int validate(const char *golden_file) {
      close(m_fd);
      String command = (String)"diff " + m_output_file + " " + golden_file;
      m_error = system(command.c_str());

      if (m_error)
        std::cerr << "Command: "<< command << " exited with "<< m_error
                  << ", see '" << m_output_file << "'" << std::endl;

      return m_error;
    }

    /** Regenerates the golden file by renaming the output file to the golden
     * file.
     *
     * @param golden_file Filename of the golden file
     */
    void regenerate_golden_file(const char *golden_file) {
      String command = (String)"mv " + m_output_file + " " + golden_file;
      HT_ASSERT(system(command.c_str()) == 0);
    }

    /** Prints an error and exits with exit code 1 */
    void display_error_and_exit() {
      close(m_fd);
      std::cerr << "Error, see '" << m_output_file << "'" << std::endl;
      _exit(1);
    }

  private:
    /** The output filename; the application name concatenated with the pid */
    char m_output_file[128];

    /** The logfile file descriptor */
    int m_fd;

    /** The error from the golden file validation */
    int m_error;
  };

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_TESTHARNESS_H

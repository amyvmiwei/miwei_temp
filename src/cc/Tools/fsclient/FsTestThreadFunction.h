/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
/// Declarations for FsTestThreadFunction.
/// This file contains declarations for FsTestThreadFunction, a thread function
/// class for running fsclient test operations.

#ifndef Tools_fsclient_FsTestThreadFunction_h
#define Tools_fsclient_FsTestThreadFunction_h

#include <FsBroker/Lib/Client.h>

#include <string>

using namespace std;

/**
 *  Forward declarations
 */
namespace Hypertable {
namespace fsclient {

  /// @addtogroup fsclient
  /// @{

  /// Thread function class for fsclient test.
  class FsTestThreadFunction {
  public:
    FsTestThreadFunction(FsBroker::Lib::ClientPtr &client, const string &input)
      : m_client(client) {
      m_input_file = input;
    }
    void set_dfs_file(const string &fname) {
      m_dfs_file = fname;
    }
    void set_output_file(const string &fname) {
      m_output_file = fname;
    }
    void operator()();

  private:
    FsBroker::Lib::ClientPtr m_client;
    string m_input_file;
    string m_output_file;
    string m_dfs_file;
  };

  /// @}

}}

#endif // Tools_fsclient_FsTestThreadFunction_h

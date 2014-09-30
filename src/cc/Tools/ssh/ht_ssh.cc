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

#include "SshSocketHandler.h"
#include "SshThreadsCallbacks.h"
#include "SshOutputCollector.h"

#include <AsyncComm/Comm.h>
#include <AsyncComm/ReactorFactory.h>

#include <Common/Error.h>
#include <Common/HostSpecification.h>
#include <Common/Init.h>
#include <Common/Logger.h>
#include <Common/Random.h>
#include <Common/System.h>
#include <Common/Timer.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#include <poll.h>

using namespace Hypertable;
using namespace std;

namespace {
  void dump_usage_and_exit() {
    cout << endl;
    cout << "ht_ssh version " << version_string() << endl;
    cout << endl;
    cout << "This application uses libssh 0.6.3 (https://www.libssh.org/)" << endl;
    cout << "libssh is licensed under the GNU Lesser General Public License" << endl;
    cout << endl;
    cout << "usage: ht_ssh [options] <hosts-specification> <command>" << endl;
    cout << endl;
    cout << "options:" << endl;
    cout << "  --debug   Turn on verbose debugging output" << endl;
    cout << "  --random-start-delay <millis>" << endl;
    cout << "            Wait a random amount of time between 0 and <millis> " << endl;
    cout << "            prior to starting each command" << endl;
    cout << endl;
    _exit(1);
  }
}

/// @defgroup ssh ssh
/// @ingroup Tools
/// Mulit-host ssh automation tool.

int main(int argc, char **argv) {

  System::initialize();
  Config::properties = new Properties();
  ReactorFactory::initialize(System::get_processor_count());

  vector<SshSocketHandlerPtr> handlers;

  string host_spec;
  string command;

  int start_delay {};
  bool debug {};
  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--debug"))
      debug = true;
    else if (!strcmp(argv[i], "--random-start-delay")) {
      i++;
      if (i == argc)
        dump_usage_and_exit();
      start_delay = atoi(argv[i]);
    }
    else if (host_spec.empty())
      host_spec = argv[i];
    else if (command.empty())
      command = argv[i];
    else
      dump_usage_and_exit();
  }

  if (command.empty())
    dump_usage_and_exit();

  vector<string> hosts;
  try {
    hosts = HostSpecification(host_spec);
  }
  catch (Exception &e) {
    cout << "Invalid host specification: " << e.what() << endl;
    _exit(1);
  }

  ssh_threads_set_callbacks(SshThreadsCallbacks::get());

  handlers.reserve(hosts.size());

  for (auto & host : hosts) {
    handlers.push_back(make_shared<SshSocketHandler>(host));
    if (debug)
      handlers.back()->enable_debug();
  }

  auto now = chrono::system_clock::now();
  auto deadline = now + std::chrono::seconds(30);

  // Wait for connections
  for (auto & handler : handlers) {
    if (!handler->wait_for_connection(deadline)) {
      handler->dump_log(cerr);
      _exit(1);
    }
  }

  vector<string> failed_hosts;

  // Issue commands

  if (start_delay) {
    map<chrono::time_point<chrono::steady_clock>, SshSocketHandlerPtr> start_map;
    chrono::time_point<chrono::steady_clock> start_time;
    for (auto & handler : handlers) {
      start_time = chrono::steady_clock::now()
        + std::chrono::milliseconds(Random::number32() % start_delay);
      start_map[start_time] = handler;
    }
    chrono::time_point<chrono::steady_clock> now;
    for (auto & entry : start_map) {
      now = chrono::steady_clock::now();
      poll(0, 0, chrono::duration_cast<std::chrono::milliseconds>(entry.first-now).count());
      if (!entry.second->issue_command(command)) {
        entry.second->dump_log(cerr);
        failed_hosts.push_back(entry.second->hostname());        
        entry.second.reset();
      }
    }
  }
  else {
    for (auto & handler : handlers) {
      if (!handler->issue_command(command)) {
        handler->dump_log(cerr);
        failed_hosts.push_back(handler->hostname());
        handler.reset();
      }
    }
  }

  // Wait for commands to complete
  for (auto & handler : handlers) {
    if (handler) {
      handler->set_terminal_output(true);
      if (!handler->wait_for_command_completion()) {
        handler->dump_log(cerr);
        failed_hosts.push_back(handler->hostname());
      }
      handler->set_terminal_output(false);
    }
  }

  if (!failed_hosts.empty()) {
    cerr << "Command failed on hosts:  ";
    bool first = true;
    for (auto & host : failed_hosts) {
      if (first)
        first = false;
      else
        cerr << ",";
      cerr << host;
    }
    cerr << endl << flush;
  }

  _exit(0);
}

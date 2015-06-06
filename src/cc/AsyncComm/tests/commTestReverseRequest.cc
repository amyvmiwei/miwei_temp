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

#include <Common/Compat.h>

#include "CommTestThreadFunction.h"

#include <AsyncComm/Comm.h>
#include <AsyncComm/Event.h>
#include <AsyncComm/ReactorFactory.h>

#include <Common/Init.h>
#include <Common/Error.h>
#include <Common/FileUtils.h>
#include <Common/TestHarness.h>
#include <Common/ServerLauncher.h>
#include <Common/StringExt.h>
#include <Common/System.h>
#include <Common/Usage.h>

#include <boost/thread/thread.hpp>

#include <chrono>
#include <cstdlib>
#include <thread>

extern "C" {
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
}

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: commTestReverseRequest",
    "",
    "This program ...",
    0
  };

}


int main(int argc, char **argv) {
  std::vector<const char *> client_args;
  std::vector<const char *> server_args;

  Config::init(argc, argv);

  if (argc != 1)
    Usage::dump_and_exit(usage);

  srand(8876);

  System::initialize(System::locate_install_dir(argv[0]));
  ReactorFactory::initialize(1);

  client_args.push_back("sampleClient");
  client_args.push_back("--recv-addr=localhost:12789");
  client_args.push_back("datafile.txt");
  client_args.push_back((const char *)0);

  server_args.push_back("testServer");
  server_args.push_back("--connect-to=localhost:12789");
  server_args.push_back((const char *)0);

  {
    ServerLauncher client("./sampleClient", (char * const *)&client_args[0],
                          "commTestReverseRequest.out");
    this_thread::sleep_for(chrono::milliseconds(2000));
    ServerLauncher server("./testServer", (char * const *)&server_args[0]);
  }

  if (system("diff commTestReverseRequest.out commTestReverseRequest.golden"))
    quick_exit(EXIT_FAILURE);

  quick_exit(EXIT_SUCCESS);
}

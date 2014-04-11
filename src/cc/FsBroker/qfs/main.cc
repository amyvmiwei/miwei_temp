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

#include "Common/Compat.h"
#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/Init.h"
#include "Common/FileUtils.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"

#include "FsBroker/Lib/Config.h"
#include "FsBroker/Lib/ConnectionHandlerFactory.h"

#include "QfsBroker.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace KFS {
  extern std::string KFS_BUILD_VERSION_STRING;
}

namespace {
  struct AppPolicy : Policy {
    static void init_options() {
    alias("port", "Qfs.MetaServer.Port");
    alias("host", "Qfs.MetaServer.Name");
    alias("workers", "Qfs.Broker.Workers");
    alias("reactors", "Qfs.Broker.Reactors");
    }
  };

  typedef Meta::list<AppPolicy, FsBrokerPolicy, DefaultCommPolicy> Policies;
}

int main(int argc, char **argv) {
  try {
    init_with_policies<Policies>(argc, argv);
    int port;
    int worker_count = get_i32("workers");

    if (has("DfsBroker.Port"))
      port = get_i16("DfsBroker.Port");
    else
      port = get_i16("FsBroker.Port");

    Comm *comm = Comm::instance();

    ApplicationQueuePtr app_queue = new ApplicationQueue(worker_count);
    BrokerPtr broker = new QfsBroker(properties);

    ConnectionHandlerFactoryPtr chfp =
        new FsBroker::ConnectionHandlerFactory(comm, app_queue, broker);
    InetAddr listen_addr(INADDR_ANY, port);

    comm->listen(listen_addr, chfp);
    app_queue->join();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
}

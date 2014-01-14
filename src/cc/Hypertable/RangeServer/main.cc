/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include "Common/FailureInducer.h"
#include "Common/Init.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"
#include "AsyncComm/ReactorRunner.h"

#include <Hypertable/Lib/ClusterId.h>

#include "Config.h"
#include "ConnectionHandler.h"
#include "Global.h"
#include "HyperspaceSessionHandler.h"
#include "RangeServer.h"
#include "IndexUpdater.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

typedef Meta::list<RangeServerPolicy, DfsClientPolicy, HyperspaceClientPolicy,
        MasterClientPolicy, DefaultServerPolicy> Policies;

int main(int argc, char **argv) {

  ReactorRunner::record_arrival_time = true;

  init_with_policies<Policies>(argc, argv);

  try {

    Global::verbose = get_bool("verbose");

    if (has("induce-failure")) {
      if (FailureInducer::instance == 0)
        FailureInducer::instance = new FailureInducer();
      FailureInducer::instance->parse_option(get_str("induce-failure"));
    }

    // issue 851: don't start if config files are missing
    if (!FileUtils::exists(System::install_dir + "/conf/METADATA.xml"))
      HT_FATALF("%s/conf/METADATA.xml is missing, aborting...",
              System::install_dir.c_str());
    if (!FileUtils::exists(System::install_dir + "/conf/RS_METRICS.xml"))
      HT_FATALF("%s/conf/RS_METRICS.xml is missing, aborting...",
              System::install_dir.c_str());

    Comm *comm = Comm::instance();
    ConnectionManagerPtr conn_manager = new ConnectionManager(comm);
    Global::conn_manager = conn_manager;

    int worker_count = get_i32("Hypertable.RangeServer.Workers");
    ApplicationQueuePtr app_queue = new ApplicationQueue(worker_count);

    /**
     * Connect to Hyperspace
     */
    HyperspaceSessionHandler hs_handler;
    Global::hyperspace = new Hyperspace::Session(comm, properties);
    Global::hyperspace->add_callback(&hs_handler);
    int hyperspace_timeout = get_i32("Hyperspace.Timeout");

    if (!Global::hyperspace->wait_for_connection(hyperspace_timeout)) {
      HT_ERROR("Unable to connect to hyperspace, exiting...");
      _exit(1);
    }

    // Initialize cluster ID from Hyperspace, enabling ClusterId::get()
    ClusterId cluster_id(Global::hyperspace);
    
    RangeServerPtr range_server= new RangeServer(properties,
        conn_manager, app_queue, Global::hyperspace);

    app_queue->join();

    IndexUpdaterFactory::close();

    range_server = 0;

    HT_ERROR("Exiting RangeServer.");
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  _exit(0);
}

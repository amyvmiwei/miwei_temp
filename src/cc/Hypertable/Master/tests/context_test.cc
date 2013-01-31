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

#include <map>

#include "Common/FailureInducer.h"
#include "Common/Init.h"
#include "Common/System.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"
#include "AsyncComm/ReactorFactory.h"

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/MetaLogReader.h"
#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Master/Context.h"
#include "Hypertable/Master/LoadBalancerBasic.h"
#include "Hypertable/Master/MetaLogDefinitionMaster.h"
#include "Hypertable/Master/OperationCreateNamespace.h"
#include "Hypertable/Master/OperationCreateTable.h"
#include "Hypertable/Master/OperationDropNamespace.h"
#include "Hypertable/Master/OperationInitialize.h"
#include "Hypertable/Master/OperationProcessor.h"
#include "Hypertable/Master/OperationRenameTable.h"
#include "Hypertable/Master/OperationSystemUpgrade.h"
#include "Hypertable/Master/OperationMoveRange.h"
#include "Hypertable/Master/ReferenceManager.h"
#include "Hypertable/Master/ResponseManager.h"

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace Config;
using namespace Hyperspace;

namespace {

  typedef Meta::list<GenericServerPolicy, DfsClientPolicy,
                     HyperspaceClientPolicy, DefaultCommPolicy> Policies;

} // local namespace


int main(int argc, char **argv) {
  ContextPtr context = new Context();
  std::vector<MetaLog::EntityPtr> entities;

  // Register ourselves as the Comm-layer proxy master
  ReactorFactory::proxy_master = true;

  context->test_mode = true;

  try {
    init_with_policies<Policies>(argc, argv);

    context->comm = Comm::instance();
    context->conn_manager = new ConnectionManager(context->comm);
    context->props = properties;
    context->hyperspace = new Hyperspace::Session(context->comm, context->props);
    context->dfs = new DfsBroker::Client(context->conn_manager, context->props);

    context->toplevel_dir = properties->get_str("Hypertable.Directory");
    boost::trim_if(context->toplevel_dir, boost::is_any_of("/"));
    context->toplevel_dir = String("/") + context->toplevel_dir;
    context->namemap = new NameIdMapper(context->hyperspace, context->toplevel_dir);
    context->monitoring = new Monitoring(context.get());

    context->mml_definition = new MetaLog::DefinitionMaster(context, "master");
    String log_dir = context->toplevel_dir + "/servers/master/log";
    context->mml_writer = new MetaLog::Writer(context->dfs,
            context->mml_definition,
            log_dir + "/" + context->mml_definition->name(), entities);

    FailureInducer::instance = new FailureInducer();
    context->request_timeout = 600;

    context->balancer = new LoadBalancerBasic(context);

    ResponseManagerContext *rmctx = new ResponseManagerContext(context->mml_writer);
    context->response_manager = new ResponseManager(rmctx);
    Thread response_manager_thread(*context->response_manager);

    context->reference_manager = new ReferenceManager();

    // test for Context::add_server
    RangeServerConnectionPtr rs1 = new RangeServerConnection(context->mml_writer,
            "rs1", "host1", InetAddr("123.1.1.1", 4000), true);
    RangeServerConnectionPtr rs2 = new RangeServerConnection(context->mml_writer,
            "rs2", "host2", InetAddr("123.1.1.2", 4000), true);
    RangeServerConnectionPtr rs3 = new RangeServerConnection(context->mml_writer,
            "rs3", "host3", InetAddr("123.1.1.3", 4000), true);
    RangeServerConnectionPtr rs4 = new RangeServerConnection(context->mml_writer,
            "rs4", "host4", InetAddr("123.1.1.4", 4000), true);

    // insert 4 unique servers
    context->add_server(rs1);
    context->add_server(rs2);
    context->add_server(rs3);
    context->add_server(rs4);

    //
    // issue 907: make sure that we do not trigger ASSERTs when connecting
    // to other RangeServers with the same IP or the same location
    //

    // add_server() of a server with a non-unique IP
    RangeServerConnectionPtr rs5 = new RangeServerConnection(context->mml_writer,
            "rs5", "host5", InetAddr("123.1.1.4", 4000), true);
    context->add_server(rs5);

    // add_server() of a server with a non-unique location
    RangeServerConnectionPtr rs6 = new RangeServerConnection(context->mml_writer,
            "rs1", "host6", InetAddr("123.1.1.6", 4000), true);
    context->add_server(rs6);

    // connect_server() of a server with a non-unique IP
    RangeServerConnectionPtr rs7 = new RangeServerConnection(context->mml_writer,
            "rs7", "host7", InetAddr("123.1.1.4", 4000), true);
    context->connect_server(rs7, "host7", InetAddr("127.0.0.1", 30000),
            InetAddr("123.1.1.4", 4000));

    // connect_server() of a server with a non-unique location
    RangeServerConnectionPtr rs8 = new RangeServerConnection(context->mml_writer,
            "rs1", "host8", InetAddr("123.1.1.8", 4000), true);
    context->connect_server(rs8, "host8", InetAddr("127.0.0.1", 30000),
            InetAddr("123.1.1.8", 4000));

    // connect_server() of another unique server
    RangeServerConnectionPtr rs9 = new RangeServerConnection(context->mml_writer,
            "rs9", "host9", InetAddr("123.1.1.9", 4000), true);
    context->connect_server(rs9, "host9", InetAddr("127.0.0.1", 30000),
            InetAddr("123.1.1.9", 4000));

    context = 0;
    _exit(0);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }
  return 0;
}

/*
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

/** @file
 * Contains main function for Master server
 * This file contains the definition for the main function for the Master
 * server
 */

#include "Common/Compat.h"

extern "C" {
#include <poll.h>
}

#include <sstream>

#include "Common/FailureInducer.h"
#include "Common/Init.h"
#include "Common/ScopeGuard.h"
#include "Common/System.h"
#include "Common/Thread.h"
#include "Common/md5.h"

#include "AsyncComm/Comm.h"

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/MetaLogReader.h"
#include "DfsBroker/Lib/Client.h"

#include "ConnectionHandler.h"
#include "Context.h"
#include "LoadBalancer.h"
#include "MetaLogDefinitionMaster.h"
#include "OperationBalance.h"
#include "OperationInitialize.h"
#include "OperationProcessor.h"
#include "OperationRecover.h"
#include "OperationRecoveryBlocker.h"
#include "OperationSystemUpgrade.h"
#include "OperationTimedBarrier.h"
#include "OperationWaitForServers.h"
#include "ReferenceManager.h"
#include "ResponseManager.h"
#include "SystemState.h"
#include "BalancePlanAuthority.h"

using namespace Hypertable;
using namespace Config;

namespace {

  struct AppPolicy : Config::Policy {
    static void init_options() {
      alias("port", "Hypertable.Master.Port");
      alias("workers", "Hypertable.Master.Workers");
      alias("reactors", "Hypertable.Master.Reactors");
    }
  };

  typedef Meta::list<GenericServerPolicy, DfsClientPolicy,
                     HyperspaceClientPolicy, DefaultCommPolicy, AppPolicy> Policies;

  class HandlerFactory : public ConnectionHandlerFactory {
  public:
    HandlerFactory(ContextPtr &context) : m_context(context) {
      m_handler = new ConnectionHandler(m_context);
    }

    virtual void get_instance(DispatchHandlerPtr &dhp) {
      dhp = m_handler;
    }

  private:
    ContextPtr m_context;
    DispatchHandlerPtr m_handler;
  };

  struct ltrsc {
    bool operator()(const RangeServerConnectionPtr &rsc1, const RangeServerConnectionPtr &rsc2) const {
      return strcmp(rsc1->location().c_str(), rsc2->location().c_str()) < 0;
    }
  };


} // local namespace

/** @defgroup Master Master
 * @ingroup Hypertable
 * %Master server.
 * The Master module contains all of the definitions that make up the Master
 * server process which is responsible for handling meta operations such
 * the following
 *   - Creating, altering, and dropping tables
 *   - %CellStore garbage collection
 *   - Orchestration of %RangeServer failover
 *   - Load balancing
 * @{
 */

/** Acquires lock on <code>/hypertable/master</code> file in hyperspace.
 * Attempts to acquire an exclusive lock on the file
 * <code>/hypertable/master</code> in Hyperspace.  The file is opened and the
 * handle is stored in the Context::master_file_handle member of
 * <code>context</code>.  If the lock is successfully acquired, the
 * <i>next_server_id</i> attribute is created and initialized to "1" if it
 * doesn't already exist.  It also creates the following directories in
 * Hyperspace if they do not already exist:
 * 
 *   - <code>/hypertable/servers</code>
 *   - <code>/hypertable/tables</code>
 *   - <code>/hypertable/root</code>
 * 
 * If the the lock is not successfully acquired, it will go into a retry loop
 * in which the function will sleep for 
 * <code>Hypertable.Connection.Retry.Interval</code> milliseconds and then
 * re-attempt to acquire the lock.
 * @param context Reference to context object
 * @note The top-level directory <code>/hypertable</code> may be different
 * depending on the <code>Hypertable.Directory</code> property.
 */
void obtain_master_lock(ContextPtr &context);

/** Writes Master's listen address to Hyperspace.
 * This method writes the address on which the Master is listening to
 * the <i>address</i> attribute of the <code>/hypertable/master</code>
 * in Hyperspace (format is IP:port).
 * @param context Reference to context object
 * @note The top-level directory <code>/hypertable</code> may be different
 * depending on the <code>Hypertable.Directory</code> property.
 */
void write_master_address(ContextPtr &context);

int main(int argc, char **argv) {
  ContextPtr context;

  // Register ourselves as the Comm-layer proxy master
  ReactorFactory::proxy_master = true;

  try {
    init_with_policies<Policies>(argc, argv);
    uint16_t port = get_i16("port");

    context = new Context(properties);

    if (properties->has("Hypertable.Cluster.Name")) {
      context->cluster_name = properties->get_str("Hypertable.Cluster.Name");
      boost::trim_if(context->cluster_name, boost::is_any_of(" '\""));
    }

    context->comm = Comm::instance();
    context->conn_manager = new ConnectionManager(context->comm);
    context->hyperspace = new Hyperspace::Session(context->comm, context->props);
    context->rsc_manager = new RangeServerConnectionManager();

    context->toplevel_dir = properties->get_str("Hypertable.Directory");
    boost::trim_if(context->toplevel_dir, boost::is_any_of("/"));
    context->toplevel_dir = String("/") + context->toplevel_dir;

    // the "state-file" signals that we're currently acquiring the hyperspace
    // lock; it is required for the serverup tool (issue 816)
    String state_file = System::install_dir + "/run/Hypertable.Master.state";
    String state_text = "acquiring";

    if (FileUtils::write(state_file, state_text) < 0)
      HT_INFOF("Unable to write state file %s", state_file.c_str());

    obtain_master_lock(context);

    if (!FileUtils::unlink(state_file))
      HT_INFOF("Unable to delete state file %s", state_file.c_str());

    context->namemap = new NameIdMapper(context->hyperspace, context->toplevel_dir);
    context->dfs = new DfsBroker::Client(context->conn_manager, context->props);
    context->mml_definition =
        new MetaLog::DefinitionMaster(context, format("%s_%u", "master", port).c_str());
    context->monitoring = new Monitoring(context.get());
    context->request_timeout = (time_t)(context->props->get_i32("Hypertable.Request.Timeout") / 1000);

    if (get_bool("Hypertable.Master.Locations.IncludeMasterHash")) {
      char location_hash[33];
      md5_string(format("%s:%u", System::net_info().host_name.c_str(), port).c_str(), location_hash);
      context->location_hash = String(location_hash).substr(0, 8);
    }
    context->max_allowable_skew = context->props->get_i32("Hypertable.RangeServer.ClockSkew.Max");
    context->monitoring_interval = context->props->get_i32("Hypertable.Monitoring.Interval");
    context->gc_interval = context->props->get_i32("Hypertable.Master.Gc.Interval");
    context->timer_interval = std::min(context->monitoring_interval, context->gc_interval);

    HT_ASSERT(context->monitoring_interval > 1000);
    HT_ASSERT(context->gc_interval > 1000);

    time_t now = time(0);
    context->next_monitoring_time = now + (context->monitoring_interval/1000) - 1;
    context->next_gc_time = now + (context->gc_interval/1000) - 1;

    if (has("induce-failure")) {
      if (FailureInducer::instance == 0)
        FailureInducer::instance = new FailureInducer();
      FailureInducer::instance->parse_option(get_str("induce-failure"));
    }

    /**
     * Read/load MML
     */
    std::vector<MetaLog::EntityPtr> entities;
    std::vector<OperationPtr> operations;
    std::map<String, OperationPtr> recovery_ops;
    MetaLog::ReaderPtr mml_reader;
    OperationPtr operation;
    RangeServerConnectionPtr rsc;
    String log_dir = context->toplevel_dir + "/servers/master/log/"
        + context->mml_definition->name();
    MetaLog::EntityPtr bpa_entity;

    mml_reader = new MetaLog::Reader(context->dfs, context->mml_definition,
            log_dir);
    mml_reader->get_entities(entities);

    // Uniq-ify the RangeServerConnection and BalancePlanAuthority objects
    {
      std::vector<MetaLog::EntityPtr> entities2;
      std::set<RangeServerConnectionPtr, ltrsc> rsc_set;

      entities2.reserve(entities.size());
      foreach_ht (MetaLog::EntityPtr &entity, entities) {
        rsc = dynamic_cast<RangeServerConnection *>(entity.get());
        if (rsc) {
          if (rsc_set.count(rsc.get()) > 0)
            rsc_set.erase(rsc.get());
          rsc_set.insert(rsc.get());
        }
        else if (dynamic_cast<BalancePlanAuthority *>(entity.get())) {
          bpa_entity = entity;
          entities2.push_back(entity);
        }
        else if (dynamic_cast<SystemState *>(entity.get())) {
          context->system_state = dynamic_cast<SystemState *>(entity.get());
          entities2.push_back(entity);
        }
        else
          entities2.push_back(entity);
      }
      foreach_ht (const RangeServerConnectionPtr &rsc, rsc_set)
        entities2.push_back(rsc.get());
      entities.swap(entities2);
    }

    if (!context->system_state)
      context->system_state = new SystemState();

    context->mml_writer = new MetaLog::Writer(context->dfs, context->mml_definition,
                                              log_dir, entities);

    if (bpa_entity) {
      BalancePlanAuthority *bpa = dynamic_cast<BalancePlanAuthority *>(bpa_entity.get());
      bpa->set_mml_writer(context->mml_writer);
      entities.push_back(bpa_entity);
      context->set_balance_plan_authority(bpa);
      std::stringstream sout;
      sout << "Loading BalancePlanAuthority: " << *bpa;
      HT_INFOF("%s", sout.str().c_str());
    }

    context->reference_manager = new ReferenceManager();

    /** Response Manager */
    ResponseManagerContext *rmctx = 
        new ResponseManagerContext(context->mml_writer);
    context->response_manager = new ResponseManager(rmctx);
    Thread response_manager_thread(*context->response_manager);

    int worker_count  = get_i32("workers");
    context->op = new OperationProcessor(context, worker_count);

    // First do System Upgrade
    operation = new OperationSystemUpgrade(context);
    context->op->add_operation(operation);
    context->op->wait_for_empty();

    // Then reconstruct state and start execution
    foreach_ht (MetaLog::EntityPtr &entity, entities) {
      operation = dynamic_cast<Operation *>(entity.get());
      if (operation) {
        if (operation->remove_explicitly())
          context->reference_manager->add(operation);
        // master was interrupted in the middle of rangeserver failover
        if (dynamic_cast<OperationRecover *>(operation.get())) {
          HT_INFO("Recovery was interrupted; continuing");
          OperationRecover *op =
            dynamic_cast<OperationRecover *>(operation.get());
          if (!op->location().empty()) {
            operations.push_back(operation);
            recovery_ops[op->location()] = operation;
          }
        }
        else
          operations.push_back(operation);
      }
      else if (dynamic_cast<RangeServerConnection *>(entity.get())) {
        rsc = dynamic_cast<RangeServerConnection *>(entity.get());
        context->rsc_manager->add_server(rsc);
      }
    }
    context->balancer = new LoadBalancer(context);

    // For each RangeServerConnection that doesn't already have an
    // outstanding OperationRecover, create and add one
    foreach_ht (MetaLog::EntityPtr &entity, entities) {
      rsc = dynamic_cast<RangeServerConnection *>(entity.get());
      if (rsc) {
        if (recovery_ops.find(rsc->location()) == recovery_ops.end())
          operations.push_back(new OperationRecover(context, rsc,
                                                    OperationRecover::RESTART));
      }
    }
    recovery_ops.clear();

    if (operations.empty()) {
      OperationInitializePtr init_op = new OperationInitialize(context);
      if (context->namemap->exists_mapping("/sys/METADATA", 0))
        init_op->set_state(OperationState::CREATE_RS_METRICS);
      context->reference_manager->add(init_op.get());
      operations.push_back( init_op );
    }
    else {
      if (context->metadata_table == 0)
        context->metadata_table = new Table(context->props,
                context->conn_manager, context->hyperspace, context->namemap,
                TableIdentifier::METADATA_NAME);

      if (context->rs_metrics_table == 0)
        context->rs_metrics_table = new Table(context->props,
                context->conn_manager, context->hyperspace, context->namemap,
                "sys/RS_METRICS");
    }

    // Add PERPETUAL operations
    operation = new OperationWaitForServers(context);
    operations.push_back(operation);
    context->recovery_barrier_op = new OperationTimedBarrier(context, Dependency::RECOVERY, Dependency::RECOVERY_BLOCKER);
    operation = new OperationRecoveryBlocker(context);
    operations.push_back(operation);

    context->op->add_operations(operations);

    ConnectionHandlerFactoryPtr hf(new HandlerFactory(context));
    InetAddr listen_addr(INADDR_ANY, port);

    context->comm->listen(listen_addr, hf);

    write_master_address(context);

    context->op->join();
    context->mml_writer->close();
    context->comm->close_socket(listen_addr);

    context->response_manager->shutdown();
    response_manager_thread.join();
    delete rmctx;
    delete context->response_manager;

    context = 0;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  return 0;
}

using namespace Hyperspace;

void obtain_master_lock(ContextPtr &context) {

  try {
    uint64_t handle = 0;
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, context->hyperspace, &handle);

    // Create TOPLEVEL directory if not exist
    context->hyperspace->mkdirs(context->toplevel_dir);

    // Create /hypertable/master if not exist
    if (!context->hyperspace->exists( context->toplevel_dir + "/master" )) {
      handle = context->hyperspace->open( context->toplevel_dir + "/master",
                                   OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE);
      context->hyperspace->close(handle);
      handle = 0;
    }

    {
      uint32_t lock_status = LOCK_STATUS_BUSY;
      uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
      LockSequencer sequencer;
      bool reported = false;
      uint32_t retry_interval = context->props->get_i32("Hypertable.Connection.Retry.Interval");

      context->master_file_handle = context->hyperspace->open(context->toplevel_dir + "/master", oflags);

      while (lock_status != LOCK_STATUS_GRANTED) {

        context->hyperspace->try_lock(context->master_file_handle, LOCK_MODE_EXCLUSIVE,
                                      &lock_status, &sequencer);

        if (lock_status != LOCK_STATUS_GRANTED) {
          if (!reported) {
            HT_INFOF("Couldn't obtain lock on '%s/master' due to conflict, entering retry loop ...",
                     context->toplevel_dir.c_str());
            reported = true;
          }
          poll(0, 0, retry_interval);
        }
      }

      HT_INFOF("Obtained lock on '%s/master'", context->toplevel_dir.c_str());

      if (!context->hyperspace->attr_exists(context->master_file_handle, "next_server_id"))
        context->hyperspace->attr_set(context->master_file_handle, "next_server_id", "1", 2);
    }

    context->hyperspace->mkdirs(context->toplevel_dir + "/servers");
    context->hyperspace->mkdirs(context->toplevel_dir + "/tables");

    // Create /hypertable/root
    handle = context->hyperspace->open(context->toplevel_dir + "/root",
        OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE);

  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }
}

void write_master_address(ContextPtr &context) {
  uint16_t port = context->props->get_i16("Hypertable.Master.Port");
  InetAddr addr(System::net_info().primary_addr, port);
  String addr_s = addr.format();
  context->hyperspace->attr_set(context->master_file_handle, "address",
                                addr_s.c_str(), addr_s.length());
  HT_INFO("Successfully Initialized.");
}


/** @}*/

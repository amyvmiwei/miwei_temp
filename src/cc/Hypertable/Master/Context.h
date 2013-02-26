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

#ifndef HYPERTABLE_CONTEXT_H
#define HYPERTABLE_CONTEXT_H

#include <set>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/thread/condition.hpp>

#include "Common/Filesystem.h"
#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/NameIdMapper.h"
#include "Hypertable/Lib/MetaLogDefinition.h"
#include "Hypertable/Lib/MetaLogWriter.h"
#include "Hypertable/Lib/Table.h"

#include "Monitoring.h"
#include "RangeServerConnection.h"
#include "RangeServerConnectionManager.h"
#include "RecoveryStepFuture.h"

namespace Hypertable {

  class LoadBalancer;
  class Operation;
  class OperationTimedBarrier;
  class OperationProcessor;
  class ResponseManager;
  class ReferenceManager;
  class BalancePlanAuthority;

  class Context : public ReferenceCount {

    class RecoveryState {
      public:
        void install_replay_future(int64_t id, RecoveryStepFuturePtr &future);
        RecoveryStepFuturePtr get_replay_future(int64_t id);
        void erase_replay_future(int64_t id);

        void install_prepare_future(int64_t id, RecoveryStepFuturePtr &future);
        RecoveryStepFuturePtr get_prepare_future(int64_t id);
        void erase_prepare_future(int64_t id);

        void install_commit_future(int64_t id, RecoveryStepFuturePtr &future);
        RecoveryStepFuturePtr get_commit_future(int64_t id);
        void erase_commit_future(int64_t id);

      private:
        friend class Context;

        typedef std::map<int64_t, RecoveryStepFuturePtr> FutureMap;

        Mutex m_mutex;
        FutureMap m_replay_map;
        FutureMap m_prepare_map;
        FutureMap m_commit_map;
    };

  public:
    Context() : timer_interval(0), monitoring_interval(0), gc_interval(0),
                next_monitoring_time(0), next_gc_time(0),
                test_mode(false), quorum_reached(false),
                m_balance_plan_authority(0) {
      master_file_handle = 0;
      balancer = 0;
      response_manager = 0;
      reference_manager = 0;
      op = 0;
      recovery_barrier_op = 0;
    }

    ~Context();

    Mutex mutex;
    boost::condition cond;
    Comm *comm;
    RangeServerConnectionManagerPtr rsc_manager;
    StringSet available_servers;
    PropertiesPtr props;
    ConnectionManagerPtr conn_manager;
    Hyperspace::SessionPtr hyperspace;
    uint64_t master_file_handle;
    FilesystemPtr dfs;
    String toplevel_dir;
    NameIdMapperPtr namemap;
    MetaLog::DefinitionPtr mml_definition;
    MetaLog::WriterPtr mml_writer;
    LoadBalancer *balancer;
    MonitoringPtr monitoring;
    ResponseManager *response_manager;
    ReferenceManager *reference_manager;
    TablePtr metadata_table;
    TablePtr rs_metrics_table;
    time_t request_timeout;
    uint32_t timer_interval;
    uint32_t monitoring_interval;
    uint32_t gc_interval;
    time_t next_monitoring_time;
    time_t next_gc_time;
    OperationProcessor *op;
    OperationTimedBarrier *recovery_barrier_op;
    String location_hash;
    int32_t max_allowable_skew;
    bool test_mode;
    bool quorum_reached;

    void add_available_server(const String &location);
    void remove_available_server(const String &location);
    size_t available_server_count();

    bool can_accept_ranges(const RangeServerStatistics &stats);
    void replay_complete(EventPtr &event);
    void prepare_complete(EventPtr &event);
    void commit_complete(EventPtr &event);

    // invoke notification hook
    void notification_hook(const String &subject, const String &message);

    // set the BalancePlanAuthority
    void set_balance_plan_authority(BalancePlanAuthority *bpa);

    // get the BalancePlanAuthority; this creates a new instance when
    // called for the very first time 
    BalancePlanAuthority *get_balance_plan_authority();

    RecoveryState &recovery_state() { return m_recovery_state; }

  private:

    RecoveryState m_recovery_state;
    BalancePlanAuthority *m_balance_plan_authority;
  };
  typedef intrusive_ptr<Context> ContextPtr;

} // namespace Hypertable

#endif // HYPERTABLE_CONTEXT_H

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

/** @file
 * Definitions for Context.
 * This file contains definitions for Context, a class that provides execution
 * context for the Master.
 */

#include <Common/Compat.h>

#include "BalancePlanAuthority.h"
#include "Context.h"
#include "LoadBalancer.h"
#include "Operation.h"
#include "OperationRecover.h"
#include "ReferenceManager.h"

#include <Hypertable/Lib/Master/Request/Parameters/PhantomCommitComplete.h>
#include <Hypertable/Lib/Master/Request/Parameters/PhantomPrepareComplete.h>
#include <Hypertable/Lib/Master/Request/Parameters/ReplayComplete.h>
#include <Hypertable/Lib/Master/Request/Parameters/ReplayStatus.h>

#include <Common/FailureInducer.h>

#include <memory>

using namespace Hypertable;
using namespace Hypertable::Lib;
using namespace std;

Context::Context(PropertiesPtr &p) 
  : props(p), timer_interval(0), monitoring_interval(0), gc_interval(0),
    next_monitoring_time(0), next_gc_time(0), test_mode(false),
    quorum_reached(false) {
  master_file_handle = 0;
  balancer = 0;
  response_manager = 0;
  reference_manager = 0;
  op = 0;
  recovery_barrier_op = 0;
  disk_threshold = props->get_i32("Hypertable.Master.DiskThreshold.Percentage");
}


Context::~Context() {
  if (hyperspace && master_file_handle > 0) {
    hyperspace->close(master_file_handle);
    master_file_handle = 0;
  }
  delete balancer;
  delete reference_manager;
}


void Context::notification_hook(const String &subject, const String &message) {

  String cmd;

  if (!cluster_name.empty())
    cmd = format("%s/conf/notification-hook.sh '[Hypertable %s] %s' '%s'", 
                 System::install_dir.c_str(), cluster_name.c_str(),
                 subject.c_str(), message.c_str());
  else
    cmd = format("%s/conf/notification-hook.sh '[Hypertable] %s' '%s'", 
                 System::install_dir.c_str(), subject.c_str(),
                 message.c_str());

  int ret = ::system(cmd.c_str());
  HT_INFOF("notification-hook returned: %d", ret);
  if (ret != 0) {
    HT_WARNF("shell script conf/notification-hook.sh ('%s') returned "
            "error %d", cmd.c_str(), ret);
  }
}

void Context::set_balance_plan_authority(MetaLog::EntityPtr bpa) {
  ScopedLock lock(mutex);
  HT_ASSERT(dynamic_cast<BalancePlanAuthority *>(bpa.get()));
  m_balance_plan_authority = bpa;
}

BalancePlanAuthority *Context::get_balance_plan_authority() {
  ScopedLock lock(mutex);
  if (!m_balance_plan_authority)
    m_balance_plan_authority = make_shared<BalancePlanAuthority>(this, mml_writer);
  return static_cast<BalancePlanAuthority *>(m_balance_plan_authority.get());
}

void Context::get_balance_plan_authority(MetaLog::EntityPtr &entity) {
  ScopedLock lock(mutex);
  if (!m_balance_plan_authority)
    m_balance_plan_authority = make_shared<BalancePlanAuthority>(this, mml_writer);
  entity = m_balance_plan_authority;
}

void Context::replay_status(EventPtr &event) {
  Master::Request::Parameters::ReplayStatus params;
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;

  params.decode(&decode_ptr, &decode_remain);

  String proxy;
  if (event->proxy == 0) {
    RangeServerConnectionPtr rsc;
    if (!rsc_manager->find_server_by_local_addr(event->addr, rsc)) {
      HT_WARNF("Unable to determine proxy for replay_status(id=%lld, %s, "
               "plan_generation=%d) from %s", (Lld)params.op_id(),
               params.location().c_str(), params.plan_generation(),
               event->addr.format().c_str());
      return;
    }
    proxy = rsc->location();
  }
  else
    proxy = event->proxy;

  HT_INFOF("replay_status(id=%lld, %s, plan_generation=%d) from %s",
           (Lld)params.op_id(), params.location().c_str(),
           params.plan_generation(), proxy.c_str());

  RecoveryStepFuturePtr future = m_recovery_state.get_replay_future(params.op_id());

  if (future)
    future->status(proxy, params.plan_generation());
  else
    HT_WARN_OUT << "No Recovery replay step future found for operation=" << params.op_id() << HT_END;

}

void Context::replay_complete(EventPtr &event) {
  Master::Request::Parameters::ReplayComplete params;
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;

  params.decode(&decode_ptr, &decode_remain);

  String proxy;
  if (event->proxy == 0) {
    RangeServerConnectionPtr rsc;
    if (!rsc_manager->find_server_by_local_addr(event->addr, rsc)) {
      HT_WARNF("Unable to determine proxy for replay_complete(id=%lld, %s, "
               "plan_generation=%d) from %s", (Lld)params.op_id(),
               params.location().c_str(), params.plan_generation(),
               event->addr.format().c_str());
      return;
    }
    proxy = rsc->location();
  }
  else
    proxy = event->proxy;

  HT_INFOF("from %s replay_complete(id=%lld, %s, plan_generation=%d) = %s",
           proxy.c_str(), (Lld)params.op_id(), params.location().c_str(),
           params.plan_generation(), Error::get_text(params.error()));

  RecoveryStepFuturePtr future
    = m_recovery_state.get_replay_future(params.op_id());

  if (future) {
    if (params.error() == Error::OK)
      future->success(proxy, params.plan_generation());
    else
      future->failure(proxy, params.plan_generation(), params.error(),
                      params.message());
  }
  else
    HT_WARN_OUT << "No Recovery replay step future found for operation="
                << params.op_id() << HT_END;

}

void Context::prepare_complete(EventPtr &event) {
  Master::Request::Parameters::PhantomPrepareComplete params;
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;

  params.decode(&decode_ptr, &decode_remain);

  HT_INFOF("prepare_complete(id=%lld, %s, plan_generation=%d) = %s",
           (Lld)params.op_id(), params.location().c_str(),
           params.plan_generation(), Error::get_text(params.error()));

  RecoveryStepFuturePtr future
    = m_recovery_state.get_prepare_future(params.op_id());

  String proxy;
  if (event->proxy == 0) {
    RangeServerConnectionPtr rsc;
    if (!rsc_manager->find_server_by_local_addr(event->addr, rsc)) {
      HT_WARNF("Unable to determine proxy for prepare_complete(id=%lld, %s, "
               "plan_generation=%d) from %s", (Lld)params.op_id(),
               params.location().c_str(), params.plan_generation(),
               event->addr.format().c_str());
      return;
    }
    proxy = rsc->location();
  }
  else
    proxy = event->proxy;

  if (future) {
    if (params.error() == Error::OK)
      future->success(proxy, params.plan_generation());
    else
      future->failure(proxy, params.plan_generation(), params.error(),
                      params.message());
  }
  else
    HT_WARN_OUT << "No Recovery prepare step future found for operation="
                << params.op_id() << HT_END;
}

void Context::commit_complete(EventPtr &event) {
  Master::Request::Parameters::PhantomPrepareComplete params;
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;

  params.decode(&decode_ptr, &decode_remain);

  HT_INFOF("commit_complete(id=%lld, %s, plan_generation=%d) = %s",
           (Lld)params.op_id(), params.location().c_str(),
           params.plan_generation(), Error::get_text(params.error()));

  RecoveryStepFuturePtr future
    = m_recovery_state.get_commit_future(params.op_id());

  String proxy;
  if (event->proxy == 0) {
    RangeServerConnectionPtr rsc;
    if (!rsc_manager->find_server_by_local_addr(event->addr, rsc)) {
      HT_WARNF("Unable to determine proxy for commit_complete(id=%lld, %s, "
               "plan_generation=%d) from %s", (Lld)params.op_id(),
               params.location().c_str(), params.plan_generation(),
               event->addr.format().c_str());
      return;
    }
    proxy = rsc->location();
  }
  else
    proxy = event->proxy;

  if (future) {
    if (params.error() == Error::OK)
      future->success(proxy, params.plan_generation());
    else
      future->failure(proxy, params.plan_generation(), params.error(),
                      params.message());
  }
  else
    HT_WARN_OUT << "No Recovery commit step future found for operation="
                << params.op_id() << HT_END;
}

bool Context::add_move_operation(OperationPtr operation) {
  ScopedLock lock(m_outstanding_move_ops_mutex);
  auto iter = m_outstanding_move_ops.find(operation->hash_code());
  if (iter != m_outstanding_move_ops.end())
    return false;
  m_outstanding_move_ops[operation->hash_code()] = operation->id();
  return true;
}

void Context::remove_move_operation(OperationPtr operation) {
  ScopedLock lock(m_outstanding_move_ops_mutex);
  auto iter = m_outstanding_move_ops.find(operation->hash_code());
  HT_ASSERT(iter != m_outstanding_move_ops.end());
  m_outstanding_move_ops.erase(iter);
}

OperationPtr Context::get_move_operation(int64_t hash_code) {
  ScopedLock lock(m_outstanding_move_ops_mutex);
  auto iter = m_outstanding_move_ops.find(hash_code);
  if (iter != m_outstanding_move_ops.end()) {
    OperationPtr operation = reference_manager->get(iter->second);
    HT_ASSERT(operation);
    return operation;
  }
  return OperationPtr();
}

void Context::add_available_server(const String &location) {
  ScopedLock lock(mutex);
  available_servers.insert(location);
}

void Context::remove_available_server(const String &location) {
  ScopedLock lock(mutex);
  available_servers.erase(location);
}

size_t Context::available_server_count() {
  ScopedLock lock(mutex);
  return available_servers.size();
}

void Context::get_available_servers(StringSet &servers) {
  ScopedLock lock(mutex);
  servers = available_servers;
}


bool Context::can_accept_ranges(const RangeServerStatistics &stats)
{

  // system info was not yet initialized; assume that the disks are available
  if (!stats.stats) {
    HT_WARNF("RangeServer %s: no disk usage statistics available",
            stats.location.c_str());
    return true;
  }

  // accept new ranges if there's at least one disk below the threshold
  double numerator=0.0, denominator=0.0;
  foreach_ht (const FsStat &fs, stats.stats->system.fs_stat) {
    numerator += fs.total - fs.avail;
    denominator += fs.total;
  }

  if (denominator == 0.0 ||
      (int32_t)((numerator/denominator)*100.00) < disk_threshold)
    return true;

  HT_WARNF("RangeServer %s: disk use %d%% exceeds threshold; will not assign "
           "ranges", stats.location.c_str(),
           (int32_t)((numerator/denominator)*100.00));

  return false;
}


void
Context::RecoveryState::install_replay_future(int64_t id,
                                              RecoveryStepFuturePtr &future) {
  ScopedLock lock(m_mutex);
  m_replay_map[id] = future;
}

RecoveryStepFuturePtr
Context::RecoveryState::get_replay_future(int64_t id) {
  ScopedLock lock(m_mutex);
  RecoveryStepFuturePtr future;
  if (m_replay_map.find(id) != m_replay_map.end())
    future = m_replay_map[id];
  return future;
}

void
Context::RecoveryState::erase_replay_future(int64_t id) {
  ScopedLock lock(m_mutex);
  m_replay_map.erase(id);
}


void
Context::RecoveryState::install_prepare_future(int64_t id,
                                               RecoveryStepFuturePtr &future) {
  ScopedLock lock(m_mutex);
  m_prepare_map[id] = future;
}

RecoveryStepFuturePtr
Context::RecoveryState::get_prepare_future(int64_t id) {
  ScopedLock lock(m_mutex);
  RecoveryStepFuturePtr future;
  if (m_prepare_map.find(id) != m_prepare_map.end())
    future = m_prepare_map[id];
  return future;
}

void
Context::RecoveryState::erase_prepare_future(int64_t id) {
  ScopedLock lock(m_mutex);
  m_prepare_map.erase(id);
}


void
Context::RecoveryState::install_commit_future(int64_t id,
                                               RecoveryStepFuturePtr &future) {
  ScopedLock lock(m_mutex);
  m_commit_map[id] = future;
}

RecoveryStepFuturePtr
Context::RecoveryState::get_commit_future(int64_t id) {
  ScopedLock lock(m_mutex);
  RecoveryStepFuturePtr future;
  if (m_commit_map.find(id) != m_commit_map.end())
    future = m_commit_map[id];
  return future;
}

void
Context::RecoveryState::erase_commit_future(int64_t id) {
  ScopedLock lock(m_mutex);
  m_commit_map.erase(id);
}


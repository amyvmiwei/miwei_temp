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
#include "Common/FailureInducer.h"

#include "Context.h"
#include "LoadBalancer.h"
#include "Operation.h"
#include "OperationRecover.h"
#include "ReferenceManager.h"
#include "BalancePlanAuthority.h"

using namespace Hypertable;
using namespace std;

Context::~Context() {
  if (hyperspace && master_file_handle > 0) {
    hyperspace->close(master_file_handle);
    master_file_handle = 0;
  }
  delete balancer;
  delete reference_manager;
  delete m_balance_plan_authority;
}


void Context::notification_hook(const String &subject, const String &message) {

  String cmd = format("%s/conf/notification-hook.sh '%s' '%s'", 
                      System::install_dir.c_str(), subject.c_str(),
                      message.c_str());

  int ret = ::system(cmd.c_str());
  HT_INFO_OUT << "notification-hook returned: " << ret << HT_END;
  if (ret != 0) {
    HT_WARNF("shell script conf/notification-hook.sh ('%s') returned "
            "error %d", cmd.c_str(), ret);
  }
}

void Context::set_balance_plan_authority(BalancePlanAuthority *bpa) {
  ScopedLock lock(mutex);
  m_balance_plan_authority = bpa;
}

BalancePlanAuthority *Context::get_balance_plan_authority() {
  ScopedLock lock(mutex);
  if (!m_balance_plan_authority)
    m_balance_plan_authority = new BalancePlanAuthority(this, mml_writer);
  return m_balance_plan_authority;
}

void Context::replay_complete(EventPtr &event) {
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;

  int64_t id       = Serialization::decode_i64(&decode_ptr, &decode_remain);
  String location  = Serialization::decode_vstr(&decode_ptr, &decode_remain);
  int plan_generation = Serialization::decode_i32(&decode_ptr, &decode_remain);
  int32_t error    = Serialization::decode_i32(&decode_ptr, &decode_remain);
  String error_msg = Serialization::decode_vstr(&decode_ptr, &decode_remain);

  HT_INFOF("replay_complete(id=%lld, %s, plan_generation=%d) = %s",
           (Lld)id, location.c_str(), plan_generation, Error::get_text(error));

  RecoveryStepFuturePtr future = m_recovery_state.get_replay_future(id);

  if (future) {
    if (error == Error::OK)
      future->success(event->proxy, plan_generation);
    else
      future->failure(event->proxy, plan_generation, error, error_msg);
  }
  else
    HT_WARN_OUT << "No Recovery replay step future found for operation=" << id << HT_END;

}

void Context::prepare_complete(EventPtr &event) {
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;

  int64_t id       = Serialization::decode_i64(&decode_ptr, &decode_remain);
  String location  = Serialization::decode_vstr(&decode_ptr, &decode_remain);
  int plan_generation = Serialization::decode_i32(&decode_ptr, &decode_remain);
  int32_t error    = Serialization::decode_i32(&decode_ptr, &decode_remain);
  String error_msg = Serialization::decode_vstr(&decode_ptr, &decode_remain);

  HT_INFOF("prepare_complete(id=%lld, %s, plan_generation=%d) = %s",
           (Lld)id, location.c_str(), plan_generation, Error::get_text(error));

  RecoveryStepFuturePtr future = m_recovery_state.get_prepare_future(id);

  if (future) {
    if (error == Error::OK)
      future->success(event->proxy, plan_generation);
    else
      future->failure(event->proxy, plan_generation, error, error_msg);
  }
  else
    HT_WARN_OUT << "No Recovery prepare step future found for operation=" << id << HT_END;
}

void Context::commit_complete(EventPtr &event) {
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;

  int64_t id       = Serialization::decode_i64(&decode_ptr, &decode_remain);
  String location  = Serialization::decode_vstr(&decode_ptr, &decode_remain);
  int plan_generation = Serialization::decode_i32(&decode_ptr, &decode_remain);
  int32_t error    = Serialization::decode_i32(&decode_ptr, &decode_remain);
  String error_msg = Serialization::decode_vstr(&decode_ptr, &decode_remain);

  HT_INFOF("commit_complete(id=%lld, %s, plan_generation=%d) = %s",
           (Lld)id, location.c_str(), plan_generation, Error::get_text(error));

  RecoveryStepFuturePtr future = m_recovery_state.get_commit_future(id);

  if (future) {
    if (error == Error::OK)
      future->success(event->proxy, plan_generation);
    else
      future->failure(event->proxy, plan_generation, error, error_msg);
  }
  else
    HT_WARN_OUT << "No Recovery commit step future found for operation=" << id << HT_END;
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



bool Context::can_accept_ranges(const RangeServerStatistics &stats)
{
  static int threshold = 0;
  if (!threshold)
    threshold = props->get_i32("Hypertable.Master.DiskThreshold.Percentage");

  // system info was not yet initialized; assume that the disks are available
  if (!stats.stats) {
    HT_WARNF("RangeServer %s: no disk usage statistics available",
            stats.location.c_str());
    return true;
  }

  // accept new ranges if there's at least one disk below the threshold
  foreach_ht (const FsStat &fs, stats.stats->system.fs_stat) {
    if (fs.use_pct < threshold)
      return true;
  }
  HT_WARNF("RangeServer %s: all disks are above threshold of %d %% "
          "(Hypertable.Master.DiskThresholdPct); will not assign ranges",
          stats.location.c_str(), threshold);
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


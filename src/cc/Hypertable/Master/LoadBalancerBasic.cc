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

#include <boost/algorithm/string.hpp>

#include "LoadBalancerBasic.h"
#include "LoadBalancerBasicDistributeLoad.h"
#include "LoadBalancerBasicDistributeTableRanges.h"
#include "LoadBalancerBasicOffloadServers.h"

#include "OperationProcessor.h"
#include "OperationBalance.h"

using namespace boost::posix_time;
using namespace Hypertable;
using namespace std;


LoadBalancerBasic::LoadBalancerBasic(ContextPtr context) : LoadBalancer(context), m_waiting_for_servers(false) {
  m_enabled = context->props->get_bool("Hypertable.LoadBalancer.Enable");
}


void LoadBalancerBasic::balance(const String &algorithm) {
  BalancePlanPtr plan = new BalancePlan;
  if (!m_enabled && algorithm == "")
    return;
  try {
    calculate_balance_plan(algorithm, plan);
    if (plan->moves.size() > 0) {
      m_context->op_balance->register_plan(plan);
      if (m_unbalanced_servers.size() > 0) {
        set_balanced();
        m_unbalanced_servers.clear();
      }
      ptime now = second_clock::local_time();
      m_last_balance_time = now;
    }
    else
      HT_DEBUG_OUT << "No balance plan created, nothing to do" << HT_END;
  }
  catch (Exception &e) {
    if (e.code()==Error::MASTER_OPERATION_IN_PROGRESS) {
      HT_INFO_OUT << e << HT_END;
      return;
    }
    HT_THROW2(e.code(), e, e.what());
  }
}

void LoadBalancerBasic::transfer_monitoring_data(vector<RangeServerStatistics> &stats) {
  ScopedLock lock(m_data_mutex);
  m_range_server_stats.swap(stats);
}

void LoadBalancerBasic::calculate_balance_plan(const String &algo, BalancePlanPtr &balance_plan) {
  String algorithm(algo);
  vector<RangeServerStatistics> range_server_stats;
  uint32_t mode = BALANCE_MODE_DISTRIBUTE_LOAD;
  ptime now = second_clock::local_time();
  set<String> offload;

  {
    time_duration td = now - m_last_balance_time;
    if (td.total_milliseconds() < m_balance_wait) {
      HT_INFO_OUT << "now=" << now << ", last balance issued at "
          << m_last_balance_time << ", wait for " << m_balance_wait
          << " before checking whether balance is " << "needed" << HT_END;
      return;
    }
    ScopedLock lock(m_data_mutex);
    range_server_stats = m_range_server_stats;
  }

  boost::trim(algorithm);
  boost::to_lower(algorithm);
  if (algorithm.size() == 0) {
    foreach_ht(const RangeServerStatistics &server_stats, range_server_stats) {
      if (!server_stats.stats->live) {
        HT_INFO_OUT << "Found non-live server " << server_stats.location
            << " wait till all servers are live before trying balance"
            << HT_END;
        return;
      }
    }
    // determine which balancer to use
    mode = BALANCE_MODE_DISTRIBUTE_LOAD;

    // when we see a new server wait for some time before scheduling a balance
    if (!m_waiting_for_servers) {
      size_t total_ranges = 0;
      get_unbalanced_servers(range_server_stats);
      size_t num_unbalanced_servers=m_unbalanced_servers.size();
      foreach_ht(const RangeServerStatistics &server_stats, range_server_stats) {
        total_ranges += server_stats.stats->range_count;
      }
      // 3 ranges shd always be in the system (2 metadata, 1 rs_metrics)
      if (num_unbalanced_servers > 0 && total_ranges > 3 +
            range_server_stats.size()) {
        HT_INFO_OUT << "Found " << num_unbalanced_servers
            << " new/unbalanced servers, "
            << "total ranges =" << total_ranges << ", total rangeservers="
            << range_server_stats.size() << HT_END;
        mode = BALANCE_MODE_DISTRIBUTE_TABLE_RANGES;
        m_wait_time_start = now;
        m_waiting_for_servers = true;
      }
    }
    else
      mode = BALANCE_MODE_DISTRIBUTE_TABLE_RANGES;
  }
  else {
    if (algorithm.compare("table_ranges")==0) {
      mode = BALANCE_MODE_DISTRIBUTE_TABLE_RANGES;
      m_waiting_for_servers = false;
    }
    else if (algorithm.compare("load")==0)
      mode = BALANCE_MODE_DISTRIBUTE_LOAD;
    else if (boost::starts_with(algorithm, "offload ")) {
      String list(algorithm, 8);
      boost::trim(list);
      boost::split(offload, list, boost::is_any_of(","));
      mode = BALANCE_MODE_OFFLOAD_SERVERS;
    }
    else
      HT_THROW(Error::NOT_IMPLEMENTED,
              (String)"Unknown LoadBalancer algorithm '" + algorithm
              + "' supported algorithms are 'TABLE_RANGES', 'LOAD'");
  }

  // TODO: write a factory class to create the sub balancer objects

  if (mode == BALANCE_MODE_DISTRIBUTE_LOAD && !m_waiting_for_servers) {
    distribute_load(now, range_server_stats, balance_plan);
  }
  else if (mode == BALANCE_MODE_DISTRIBUTE_TABLE_RANGES) {
    time_duration td = now - m_wait_time_start;
    if (m_waiting_for_servers && td.total_milliseconds() > m_balance_wait)
      m_waiting_for_servers = false;
    if (!m_waiting_for_servers)
      distribute_table_ranges(range_server_stats, balance_plan);
  }
  else if (mode == BALANCE_MODE_OFFLOAD_SERVERS) {
    offload_servers(range_server_stats, offload, balance_plan);
  }

  if (balance_plan->moves.size()) {
    get_unbalanced_servers(range_server_stats);
    if (mode == BALANCE_MODE_DISTRIBUTE_LOAD) {
      HT_INFO_OUT << "LoadBalancerBasic mode=BALANCE_MODE_DISTRIBUTE_LOAD"
          << HT_END;
    }
    else
      HT_INFO_OUT << "LoadBalancerBasic mode=BALANCE_MODE_DISTRIBUTE_TABLE_RANGES"
          << HT_END;
    HT_INFO_OUT << "BalancePlan created, move " << balance_plan->moves.size()
        << " ranges" << HT_END;
  }
}

void LoadBalancerBasic::distribute_load(const ptime &now,
        vector<RangeServerStatistics> &range_server_stats,
        BalancePlanPtr &balance_plan) {
  // check to see if m_balance_interval time has passed since the last balance
  time_duration td = now - m_last_balance_time;
  if (td.total_milliseconds() < m_balance_interval)
    return;
  // check to see if it is past time time of day when the balance should occur
  td = now.time_of_day();
  if (td < m_balance_window_start || td > m_balance_window_end)
    return;

  HT_INFO_OUT << "Current time = " << td << ", m_balance_window_start="
      << m_balance_window_start << ", m_balance_window_end="
      << m_balance_window_end << HT_END;

  LoadBalancerBasicDistributeLoad planner(m_balance_loadavg_threshold,
          range_server_stats, m_context);
  planner.compute_plan(balance_plan);
}

void LoadBalancerBasic::distribute_table_ranges(vector<RangeServerStatistics> &range_server_stats,
        BalancePlanPtr &balance_plan) {
  // no need to check if its time to do balance, we have empty servers,
  // so do balance
  LoadBalancerBasicDistributeTableRanges planner(m_context);
  planner.compute_plan(range_server_stats, balance_plan);
}

void LoadBalancerBasic::offload_servers(vector<RangeServerStatistics> &range_server_stats,
        set<String> &offload, BalancePlanPtr &balance_plan) {
  // no need to check if its time to do balance, we have empty servers,
  // so do balance
  LoadBalancerBasicOffloadServers planner(m_context);
  String root_location;
  get_root_location(root_location);
  planner.compute_plan(range_server_stats, offload, root_location, balance_plan);
}

void LoadBalancerBasic::get_unbalanced_servers(const vector<RangeServerStatistics> &range_server_stats) {
  m_unbalanced_servers.clear();
  vector<String> servers;
  foreach_ht(const RangeServerStatistics &server_stats, range_server_stats)
    servers.push_back(server_stats.location);
  m_context->get_unbalanced_servers(servers, m_unbalanced_servers);
}

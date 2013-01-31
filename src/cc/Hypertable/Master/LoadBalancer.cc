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

#include "BalanceAlgorithmEvenRanges.h"
#include "BalanceAlgorithmLoad.h"
#include "BalanceAlgorithmOffload.h"
#include "LoadBalancer.h"
#include "Utility.h"

#include <cstring>

using namespace Hypertable;

LoadBalancer::LoadBalancer(ContextPtr context)
  : m_context(context), m_new_server_added(false), m_paused(false) {

  m_crontab = new Crontab( m_context->props->get_str("Hypertable.LoadBalancer.Crontab") );

  m_new_server_balance_delay =
    m_context->props->get_i32("Hypertable.LoadBalancer.BalanceDelay.NewServer");

  // TODO: fix me!!
  //m_enabled = context->props->get_bool("Hypertable.LoadBalancer.Enable");
  m_enabled = false;

  m_loadavg_threshold = 
            m_context->props->get_f64("Hypertable.LoadBalancer.LoadavgThreshold");

  time_t t = time(0) +
    m_context->props->get_i32("Hypertable.LoadBalancer.BalanceDelay.Initial");

  m_next_balance_time_load = m_crontab->next_event(t);

  if (m_context->rsc_manager->exist_unbalanced_servers()) {
    m_new_server_added = true;
    m_next_balance_time_new_server = time(0) + m_new_server_balance_delay;
  }
  else
    m_next_balance_time_new_server = 0;
}

void LoadBalancer::signal_new_server() {
  ScopedLock lock(m_add_mutex);
  m_new_server_added = true;
  m_next_balance_time_new_server = time(0) + m_new_server_balance_delay;
}

bool LoadBalancer::balance_needed() {
  ScopedLock lock(m_add_mutex);
  time_t now = time(0);
  if (m_paused)
    return false;
  if (m_new_server_added && now >= m_next_balance_time_new_server)
    return true;
  if (m_enabled && now >= m_next_balance_time_load)
    return true;
  return false;
}


void LoadBalancer::unpause() {
  ScopedLock lock(m_add_mutex);
  time_t now = time(0);
  m_new_server_added = false;
  if (m_context->rsc_manager->exist_unbalanced_servers()) {
    m_new_server_added = true;
    m_next_balance_time_new_server = now + m_new_server_balance_delay;
  }
  m_next_balance_time_load = m_crontab->next_event(now);
  m_paused = false;
}


void LoadBalancer::transfer_monitoring_data(vector<RangeServerStatistics> &stats) {
  ScopedLock lock(m_add_mutex);
  m_statistics.swap(stats);
}


void LoadBalancer::create_plan(BalancePlanPtr &plan,
                               std::vector <RangeServerConnectionPtr> &balanced) {
  String name, arguments;
  time_t now = time(0);
  BalanceAlgorithmPtr algo;

  {
    ScopedLock lock(m_add_mutex);
    String algorithm_spec = plan->algorithm;

    if (m_statistics.empty())
      HT_THROW(Error::MASTER_BALANCE_PREVENTED, "Statistics vector is empty");

    if (m_statistics.size() == 1) {
      if (m_context->rsc_manager->server_count() == 1) {
        if (m_new_server_added) {
          RangeServerConnectionPtr rsc;
          if (m_context->rsc_manager->find_server_by_location(m_statistics[0].location, rsc))
            balanced.push_back(rsc);
        }
        plan->clear();
        m_paused = true;
        return;
      }
      HT_THROW(Error::MASTER_BALANCE_PREVENTED, "Not enough server statistics");
    }

    /**
     * Split algorithm spec into algorithm name + arguments
     */
    boost::trim(algorithm_spec);
    if (algorithm_spec == "") {
      if (m_new_server_added && now >= m_next_balance_time_new_server)
        name = "table_ranges";
      else if (now >= m_next_balance_time_load)
        name = "load";
      else
        HT_THROW(Error::MASTER_BALANCE_PREVENTED, "Balance not needed");
    }
    else {
      char *ptr = strchr((char *)algorithm_spec.c_str(), ' ');
      if (ptr) {
        name = String(algorithm_spec, 0, ptr-algorithm_spec.c_str());
        if (*ptr) {
          arguments = String(ptr);
          boost::trim(arguments);
        }
      }
      else
        name = algorithm_spec;
      boost::to_lower(name);
    }

    HT_INFOF("LoadBalance(name='%s', args='%s')", name.c_str(), arguments.c_str());

    if (name == "offload")
      algo = new BalanceAlgorithmOffload(m_context, m_statistics, arguments);
    else if (name == "table_ranges")
      algo = new BalanceAlgorithmEvenRanges(m_context, m_statistics);
    else if (name == "load")
      algo = new BalanceAlgorithmLoad(m_context, m_statistics);
    else
      HT_THROWF(Error::MASTER_BALANCE_PREVENTED,
                "Unrecognized algorithm - %s", name.c_str());

    plan->algorithm = name;
  }

  algo->compute_plan(plan, balanced);

  {
    ScopedLock lock(m_add_mutex);
    m_paused = true;
  }
}

void Hypertable::reenable_balancer(LoadBalancer *balancer) {
  
}

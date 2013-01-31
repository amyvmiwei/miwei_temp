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

#ifndef HYPERTABLE_LOADBALANCER_H
#define HYPERTABLE_LOADBALANCER_H

#include "Common/Crontab.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/BalancePlan.h"

#include "RangeServerConnection.h"
#include "RangeServerStatistics.h"
#include "Context.h"

#include <vector>
#include <time.h>

namespace Hypertable {

  class LoadBalancer : public ReferenceCount {
  public:
    LoadBalancer(ContextPtr context);

    void signal_new_server();

    bool balance_needed();

    void unpause();

    void create_plan(BalancePlanPtr &plan,
                     std::vector <RangeServerConnectionPtr> &balanced);

    void transfer_monitoring_data(vector<RangeServerStatistics> &stats);

  private:
    Mutex m_mutex;
    ContextPtr m_context;
    Mutex m_add_mutex;
    CrontabPtr m_crontab;
    time_t m_next_balance_time_load;
    time_t m_next_balance_time_new_server;
    double m_loadavg_threshold;
    uint32_t m_new_server_balance_delay;
    bool m_new_server_added;
    bool m_enabled;
    bool m_paused;
    std::vector <RangeServerStatistics> m_statistics;
  };

  typedef intrusive_ptr<LoadBalancer> LoadBalancerPtr;

  void reenable_balancer(LoadBalancer *balancer);
  

} // namespace Hypertable

#endif // HYPERTABLE_LOADBALANCER_H

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

#ifndef HYPERTABLE_LOADBALANCERBASIC_H
#define HYPERTABLE_LOADBALANCERBASIC_H

#include <map>
#include <set>
#include <iostream>


#include "Common/String.h"

#include "LoadBalancer.h"
#include "RSMetrics.h"


namespace Hypertable {

  class LoadBalancerBasic : public LoadBalancer {
    public:
    enum {
      BALANCE_MODE_DISTRIBUTE_LOAD             = 1,
      BALANCE_MODE_DISTRIBUTE_TABLE_RANGES     = 2,
      BALANCE_MODE_OFFLOAD_SERVERS             = 3
    };
    LoadBalancerBasic(ContextPtr context);

      void transfer_monitoring_data(vector<RangeServerStatistics> &stats);
      void balance(const String &algorithm=String());

      //String assign_to_server(TableIdentifier &tid, RangeIdentifier &rid) = 0;
      //void range_move_loaded(TableIdentifier &tid, RangeIdentifier &rid) = 0;
      //void range_relinquish_acknowledged(TableIdentifier &tid, RangeIdentifier &rid) = 0;
      //time_t maintenance_interval() = 0;
    private:
      void calculate_balance_plan(const String &algorithm, BalancePlanPtr &plan);
      void distribute_load(const boost::posix_time::ptime &now, BalancePlanPtr &plan);
      void distribute_table_ranges(vector<RangeServerStatistics> &range_server_stats,
                                   BalancePlanPtr &plan);
      void offload_servers(vector<RangeServerStatistics> &range_server_stats,
                           set<String> &offload, BalancePlanPtr &balance_plan);

      void get_unbalanced_servers(const std::vector<RangeServerStatistics> &stats);

      Mutex m_data_mutex;
      bool m_enabled;
      bool  m_waiting_for_servers;
      std::vector <RangeServerStatistics> m_range_server_stats;
      ptime m_wait_time_start;
  }; // LoadBalancerBasic

} // namespace Hypertable

#endif // HYPERTABLE_LOADBALANCERBASIC_H

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

#ifndef HYPERTABLE_LOADBALANCEROFFLOADSERVERS_H
#define HYPERTABLE_LOADBALANCEROFFLOADSERVERS_H

#include <vector>
#include <set>

#include "Common/StringExt.h"

#include "Hypertable/Lib/BalancePlan.h"
#include "Hypertable/Lib/Client.h"
#include "RangeServerStatistics.h"

namespace Hypertable {

  // Moves ranges off given set of servers by round robin distributing over the
  // remaining servers
  class LoadBalancerBasicOffloadServers {

    public:
      LoadBalancerBasicOffloadServers(TablePtr &table) : m_table(table) { }
      void compute_plan(std::vector<RangeServerStatistics> &range_server_stats,
                        std::set<String> &offload_servers, const String &root_location,
                        BalancePlanPtr &balance_plan);
    private:
      void compute_moves(std::vector<RangeServerStatistics> &range_server_stats,
                         std::set<String> &offload_servers, const String &root_location,
                         BalancePlanPtr &balance_plan);

      TablePtr m_table;
  }; // LoadBalancerBasicOffloadServers


} // namespace Hypertable

#endif // HYPERTABLE_LOADBALANCEROFFLOADSERVERS_H

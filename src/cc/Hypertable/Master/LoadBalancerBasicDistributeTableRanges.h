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

#ifndef HYPERTABLE_LOADBALANCERBASICDISTRIBUTETABLERANGES_H
#define HYPERTABLE_LOADBALANCERBASICDISTRIBUTETABLERANGES_H

#include <vector>
#include <map>

#include "Common/StringExt.h"

#include "Hypertable/Lib/BalancePlan.h"
#include "Hypertable/Lib/Client.h"
#include "RangeServerStatistics.h"

namespace Hypertable {

  class LoadBalancerBasicDistributeTableRanges {

    public:
      LoadBalancerBasicDistributeTableRanges(TablePtr &table) : m_table(table),
          m_num_servers(0), m_num_ranges(0) { }
      void compute_plan(std::vector<RangeServerStatistics> &range_server_stats,
                        BalancePlanPtr &balance_plan);
    private:
      void clear();
      void compute_moves(BalancePlanPtr &plan);
      bool maybe_add_to_plan(const char *table, const char *src_server, const char *start_row,
                             const char *end_row, BalancePlanPtr &plan);
      void compute_range_distribution(vector<RangeServerStatistics> &range_server_stats);

      // map of server_id to number of ranges
      typedef std::map<const char *, uint32_t, LtCstr> RangeDistributionMap;
      class TableSummary : public ReferenceCount {
      public:
        TableSummary() : total_ranges(0), ranges_per_server(0) { }
        uint32_t total_ranges;
        uint32_t ranges_per_server;
        RangeDistributionMap range_dist;
      };
      typedef intrusive_ptr<TableSummary> TableSummaryPtr;
      // map of table_id to TableSummary
      typedef std::map<const char *, TableSummaryPtr, LtCstr> TableSummaryMap;

      vector<const char *> m_servers;
      CstrSet m_saturated_tables;
      TableSummaryMap m_table_summaries;
      TablePtr m_table;
      uint32_t m_num_servers;
      size_t m_num_ranges;
      uint32_t m_num_empty_servers;
  }; // LoadBalancerBasicDistributeTableRanges


} // namespace Hypertable

#endif // HYPERTABLE_LOADBALANCERBASICDISTRIBUTETABLERANGES_H

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

#include "LoadBalancerBasicDistributeLoad.h"

using namespace Hypertable;
using namespace std;

void LoadBalancerBasicDistributeLoad::compute_plan(BalancePlanPtr &balance_plan) {
  vector<ServerMetrics> server_metrics;
  RSMetrics rs_metrics(m_context->rs_metrics_table);
  rs_metrics.get_server_metrics(server_metrics);

  ServerMetricSummary ss;
  ServerSetDescLoad servers_desc_load;
  int num_servers;
  int num_loaded_servers = 0;
  double mean_loadavg = 0;
  double mean_loadavg_per_loadestimate = 0;
  String heaviest_server_id, lightest_server_id;

  foreach_ht(const ServerMetrics &sm, server_metrics) {
    calculate_server_summary(sm, ss);
    servers_desc_load.insert(ss);
    mean_loadavg += ss.loadavg;
    if (ss.loadavg_per_loadestimate > 0) {
      mean_loadavg_per_loadestimate += ss.loadavg_per_loadestimate;
      num_loaded_servers++;
    }
  }
  num_servers = servers_desc_load.size();

  if (num_servers < 2 || num_loaded_servers < 1) {
    HT_INFO_OUT << "No balancing required, num_servers=" << num_servers
        << ", num_loaded_servers=" << num_loaded_servers << HT_END;
    return;
  }

  mean_loadavg /= num_servers;
  mean_loadavg_per_loadestimate /= num_loaded_servers;
  HT_INFO_OUT << "mean_loadavg=" << mean_loadavg
      << ", num_servers=" << num_servers
      << ", mean_loadavg_per_loadestimate=" << mean_loadavg_per_loadestimate
      << ", num_loaded_servers=" << num_loaded_servers
      << ", loadavg_deviation_threshold=" << m_loadavg_deviation_threshold
      << HT_END;

  while (1) {
    if (servers_desc_load.size() < 2)
      break;
    ServerMetricSummary heaviest_server = *(servers_desc_load.begin());

    // find the lightest server with enough disk capacity 
    ServerSetDescLoad::reverse_iterator rit = servers_desc_load.rbegin();
    while (rit != servers_desc_load.rend() && (*rit).disk_full)
      ++rit;
    if (rit == servers_desc_load.rend())
      break;
    ServerMetricSummary lightest_server = *rit;

    if (lightest_server.loadavg_per_loadestimate == 0)
      lightest_server.loadavg_per_loadestimate = mean_loadavg_per_loadestimate;
    if (heaviest_server.loadavg_per_loadestimate == 0)
      heaviest_server.loadavg_per_loadestimate = mean_loadavg_per_loadestimate;

    HT_DEBUG_OUT << "heaviest_server=" << heaviest_server
        << ", lightest_server=" << lightest_server << HT_END;

    // the heaviest server doesnt have enough load to justify any more moves,
    // so we're done
    if (heaviest_server.loadavg < m_loadavg_deviation_threshold + mean_loadavg) {
      HT_DEBUG_OUT << "Heaviest loaded server now has estimated loadavg of "
          << heaviest_server.loadavg << " which is within the acceptable threshold ("
          << m_loadavg_deviation_threshold << ") of the mean_loadavg " << mean_loadavg
          << HT_END;
      break;
    }

    RangeMetricsMap range_metrics;
    RangeSetDescLoad ranges_desc_load;

    rs_metrics.get_range_metrics(heaviest_server.server_id, range_metrics);
    populate_range_load_set(range_metrics, ranges_desc_load);

    RangeSetDescLoad::iterator ranges_desc_load_it = ranges_desc_load.begin();

    while (heaviest_server.loadavg > m_loadavg_deviation_threshold + mean_loadavg &&
           ranges_desc_load_it != ranges_desc_load.end()) {
      if (check_move(heaviest_server, lightest_server,
                  ranges_desc_load_it->loadestimate, mean_loadavg)) {
        // add move to balance plan
        RangeMoveSpecPtr move = new RangeMoveSpec(heaviest_server.server_id,
            lightest_server.server_id, ranges_desc_load_it->table_id,
            ranges_desc_load_it->start_row, ranges_desc_load_it->end_row);
        HT_DEBUG_OUT << "Added move to plan: " << *(move.get()) << HT_END;
        balance_plan->moves.push_back(move);

        // recompute loadavgs
        heaviest_server.loadavg -=
            heaviest_server.loadavg_per_loadestimate * ranges_desc_load_it->loadestimate;
        heaviest_server.loadavg = (heaviest_server.loadavg < 0) ? 0 : heaviest_server.loadavg;
        lightest_server.loadavg +=
          lightest_server.loadavg_per_loadestimate * ranges_desc_load_it->loadestimate;

        // erase old lightest server and reinsert with new loadavg estimate
        ServerSetDescLoad::iterator it = servers_desc_load.end();
        --it;
        servers_desc_load.erase(it);
        servers_desc_load.insert(lightest_server);
        lightest_server = *(servers_desc_load.rbegin());

        // no need to erase this range from the heaviest loaded range
        // since we will skip to next range and delete the heaviest server from
        // the set of servers used in balancing after all moves are done for it
      }
      else {
        HT_DEBUG_OUT << "Moving range " << *ranges_desc_load_it
            << " is not viable." << HT_END;
      }
      ranges_desc_load_it++;
    }

    // erase old heaviest server, since it won't be a source or destination
    // for balancing anymore
    servers_desc_load.erase(servers_desc_load.begin());
  }
}

void LoadBalancerBasicDistributeLoad::calculate_server_summary(const ServerMetrics &metrics,
        ServerMetricSummary &summary) {
  summary.server_id = metrics.get_id().c_str();
  double loadestimate = 0;

  // calculate average loadavg for server
  const vector<ServerMeasurement> &measurements = metrics.get_measurements();
  if (measurements.size() > 0) {
    foreach_ht(const ServerMeasurement& measurement, measurements) {
      summary.loadavg += measurement.loadavg;
      loadestimate += measurement.bytes_written_rate
          + measurement.bytes_scanned_rate;
    }
    summary.loadavg /= measurements.size();
    summary.loadavg_per_loadestimate = summary.loadavg
        / (loadestimate / measurements.size());
  }

  StatisticsSet::iterator it = m_rsstats.find(metrics.get_id());
  if (it != m_rsstats.end())
    summary.disk_full = !m_context->can_accept_ranges(it->second);
}

void LoadBalancerBasicDistributeLoad::calculate_range_summary(const RangeMetrics &metrics,
    RangeMetricSummary &summary) {

  bool start_row_set;
  summary.table_id  = metrics.get_table_id().c_str();
  summary.start_row = metrics.get_start_row(&start_row_set).c_str();
  summary.end_row   = metrics.get_end_row().c_str();

  // calculate the average loadestimate for this range
  const vector<RangeMeasurement> &measurements = metrics.get_measurements();
  if (measurements.size() > 0) {
    foreach_ht(const RangeMeasurement &measurement, measurements)
      summary.loadestimate += measurement.byte_read_rate + measurement.byte_write_rate;
    summary.loadestimate /= measurements.size();
  }
}


void LoadBalancerBasicDistributeLoad::populate_range_load_set(const RangeMetricsMap &range_metrics, RangeSetDescLoad &ranges_desc_load) {

  ranges_desc_load.clear();
  foreach_ht(const RangeMetricsMap::value_type &vv, range_metrics) {
    // don't consider ranges that can't be moved
    if (!vv.second.is_moveable())
      continue;
    RangeMetricSummary summary;
    calculate_range_summary(vv.second, summary);
    ranges_desc_load.insert(summary);
  }
}

bool LoadBalancerBasicDistributeLoad::check_move(const ServerMetricSummary &source,
    const ServerMetricSummary &destination, double range_loadestimate,
    double mean_loadavg) {
  // make sure that this move doesn't increase the loadavg of the target more than that of the source
  double loadavg_destination = destination.loadavg;
  double delta_destination;

  delta_destination = destination.loadavg_per_loadestimate * range_loadestimate;
  loadavg_destination += delta_destination;
  return (loadavg_destination < m_loadavg_deviation_threshold + mean_loadavg);
}

ostream &Hypertable::operator<<(ostream &out,
    const LoadBalancerBasicDistributeLoad::ServerMetricSummary &summary) {
  out << "{ServerMetricSummary: server_id=" << summary.server_id << ", loadavg="
      << summary.loadavg << ", loadavg_per_loadestimate=" << summary.loadavg_per_loadestimate
      << "}";
  return out;
}

ostream &Hypertable::operator<<(ostream &out,
    const LoadBalancerBasicDistributeLoad::RangeMetricSummary &summary) {
  out << "{RangeMetricSummary: table_id=" << summary.table_id << ", start_row="
      << summary.start_row  << ", end_row=" << summary.end_row
      << ", loadestimate=" << summary.loadestimate << "}";
  return out;
}


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

#include "BalanceAlgorithmLoad.h"

using namespace Hypertable;
using namespace std;


BalanceAlgorithmLoad::BalanceAlgorithmLoad(ContextPtr &context,
                                           std::vector<RangeServerStatistics> &statistics)
  : m_context(context) {

  m_loadavg_deviation_threshold = m_context->props->get_f64("Hypertable.LoadBalancer.LoadavgThreshold");

  foreach_ht (RangeServerStatistics &rs, statistics)
    m_rsstats[rs.location] = rs;
}


void BalanceAlgorithmLoad::compute_plan(BalancePlanPtr &plan,
                                        std::vector<RangeServerConnectionPtr> &balanced) {
  vector<ServerMetrics> server_metrics;
  RSMetrics rs_metrics(m_context->rs_metrics_table);
  rs_metrics.get_server_metrics(server_metrics);

  ServerMetricSummary ss;
  ServerSetDescLoad servers_desc_load;
  int num_servers;
  int num_loaded_servers=0;
  double mean_loadavg=0;
  double mean_loadavg_per_loadestimate=0;

  foreach_ht(const ServerMetrics &sm, server_metrics) {
    // only assign ranges if this RangeServer is connected
    RangeServerConnectionPtr rsc;
    if (m_context->rsc_manager &&
        (!m_context->rsc_manager->find_server_by_location(sm.get_id(), rsc)
         || !rsc->connected() || rsc->get_removed() || rsc->is_recovering())) {
      HT_INFOF("RangeServer %s not connected, skipping", sm.get_id().c_str());
      continue;
    }

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
  HT_INFO_OUT << "mean_loadavg=" << mean_loadavg << ", num_servers="
      << num_servers << ", mean_loadavg_per_loadestimate="
      << mean_loadavg_per_loadestimate << ", num_loaded_servers="
      << num_loaded_servers << ", loadavg_deviation_threshold="
      << m_loadavg_deviation_threshold << HT_END;

  while (1) {
    if (servers_desc_load.size() < 2)
      break;
    ServerMetricSummary heaviest = *(servers_desc_load.begin());
    ServerMetricSummary lightest = *(servers_desc_load.rbegin());

    if (lightest.loadavg_per_loadestimate == 0)
      lightest.loadavg_per_loadestimate = mean_loadavg_per_loadestimate;
    if (heaviest.loadavg_per_loadestimate == 0)
      heaviest.loadavg_per_loadestimate = mean_loadavg_per_loadestimate;

    HT_DEBUG_OUT << "heaviest_server=" << heaviest << ", lightest_server="
        << lightest << HT_END;

    // the heaviest server doesnt have enough load to justify any more moves,
    // so we're done
    if (heaviest.loadavg < m_loadavg_deviation_threshold + mean_loadavg) {
      HT_DEBUG_OUT << "Heaviest loaded server now has estimated loadavg of "
          << heaviest.loadavg << " which is within the acceptable threshold ("
          << m_loadavg_deviation_threshold << ") of the mean_loadavg " << mean_loadavg
          << HT_END;
      break;
    }

    RangeMetricsMap range_metrics;
    RangeSetDescLoad ranges_desc_load;

    rs_metrics.get_range_metrics(heaviest.server_id, range_metrics);
    populate_range_load_set(range_metrics, ranges_desc_load);

    RangeSetDescLoad::iterator ranges_desc_load_it = ranges_desc_load.begin();

    while (heaviest.loadavg > m_loadavg_deviation_threshold + mean_loadavg &&
           ranges_desc_load_it != ranges_desc_load.end()) {
      if (check_move(heaviest, lightest, ranges_desc_load_it->loadestimate,
                     mean_loadavg)) {
        // add move to balance plan
        RangeMoveSpecPtr move = new RangeMoveSpec(heaviest.server_id,
            lightest.server_id, ranges_desc_load_it->table_id,
            ranges_desc_load_it->start_row, ranges_desc_load_it->end_row);
        HT_DEBUG_OUT << "Added move to plan: " << *(move.get()) << HT_END;
        plan->moves.push_back(move);

        // recompute loadavgs
        heaviest.loadavg -=
            heaviest.loadavg_per_loadestimate * ranges_desc_load_it->loadestimate;
        heaviest.loadavg = (heaviest.loadavg < 0) ? 0 : heaviest.loadavg;
        lightest.loadavg +=
          lightest.loadavg_per_loadestimate * ranges_desc_load_it->loadestimate;

        // erase old lightest server and reinsert with new loadavg estimate
        ServerSetDescLoad::iterator it = servers_desc_load.end();
        --it;
        servers_desc_load.erase(it);
        servers_desc_load.insert(lightest);
        lightest = *(servers_desc_load.rbegin());

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

void BalanceAlgorithmLoad::calculate_server_summary(const ServerMetrics &metrics,
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

void BalanceAlgorithmLoad::calculate_range_summary(const RangeMetrics &metrics,
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


void BalanceAlgorithmLoad::populate_range_load_set(const RangeMetricsMap &range_metrics, RangeSetDescLoad &ranges_desc_load) {

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

bool BalanceAlgorithmLoad::check_move(const ServerMetricSummary &source,
    const ServerMetricSummary &destination, double range_loadestimate,
    double mean_loadavg) {
  // make sure that this move doesn't increase the loadavg of the target more than that of the source
  double loadavg_destination = destination.loadavg;
  double delta_destination;

  delta_destination = destination.loadavg_per_loadestimate * range_loadestimate;
  loadavg_destination += delta_destination;
  return (loadavg_destination < m_loadavg_deviation_threshold + mean_loadavg);
}

/** @relates BalanceAlgorithmLoad::ServerMetricSummary */
ostream &Hypertable::operator<<(ostream &out,
    const BalanceAlgorithmLoad::ServerMetricSummary &summary) {
  out << "{ServerMetricSummary: server_id=" << summary.server_id << ", loadavg="
      << summary.loadavg << ", loadavg_per_loadestimate=" << summary.loadavg_per_loadestimate
      << "}";
  return out;
}

/** @relates BalanceAlgorithmLoad::RangeMetricSummary */
ostream &Hypertable::operator<<(ostream &out,
    const BalanceAlgorithmLoad::RangeMetricSummary &summary) {
  out << "{RangeMetricSummary: table_id=" << summary.table_id << ", start_row="
      << summary.start_row  << ", end_row=" << summary.end_row
      << ", loadestimate=" << summary.loadestimate << "}";
  return out;
}


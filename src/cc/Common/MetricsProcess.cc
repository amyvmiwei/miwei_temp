/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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

/// @file
/// Declarations for MetricsProcess.
/// This file contains type declarations for MetricsProcess, a simple class for
/// aggregating metrics and sending them to the Ganglia gmond process running on
/// localhost.

#include <Common/Compat.h>

#include "MetricsProcess.h"

#include <Common/Logger.h>
#include <Common/StatsSystem.h>
#include <Common/Time.h>

using namespace Hypertable;
using namespace std;

MetricsProcess::MetricsProcess() {
  StatsSystem system_stats(StatsSystem::PROCINFO|StatsSystem::PROC);
  system_stats.refresh();
  m_last_timestamp = get_ts64();
  m_last_sys = system_stats.proc_stat.cpu_sys;
  m_last_user = system_stats.proc_stat.cpu_user;
}

void MetricsProcess::collect(int64_t now, MetricsCollector *collector) {
  StatsSystem system_stats(StatsSystem::PROCINFO|StatsSystem::PROC);

  system_stats.refresh();

  int64_t elapsed_millis = (now - m_last_timestamp) / 1000000LL;
  int64_t diff_sys = (system_stats.proc_stat.cpu_sys - m_last_sys) / System::cpu_info().total_cores;
  int64_t diff_user = (system_stats.proc_stat.cpu_user - m_last_user) / System::cpu_info().total_cores;
  int32_t pct = 0;

  // CPU sys
  if (elapsed_millis)
    pct = (diff_sys * 100) / elapsed_millis;
  collector->update("cpu.sys", pct);

  // CPU user
  if (elapsed_millis)
    pct = (diff_user * 100) / elapsed_millis;
  collector->update("cpu.user", pct);

  // Virtual memory
  collector->update("memory.virtual",
                    (float)system_stats.proc_stat.vm_size / 1024.0);

  // Resident memory
  collector->update("memory.resident",
                    (float)system_stats.proc_stat.vm_resident / 1024.0);

  // Heap size
  collector->update("memory.heap",
                    (float)system_stats.proc_stat.heap_size / 1000000000.0);

  // Heap slack bytes
  collector->update("memory.heapSlack",
                    (float)system_stats.proc_stat.heap_slack / 1000000000.0);

  // Hypertable version
  collector->update("version", version_string());

  m_last_timestamp = now;
  m_last_sys = system_stats.proc_stat.cpu_sys;
  m_last_user = system_stats.proc_stat.cpu_user;

}

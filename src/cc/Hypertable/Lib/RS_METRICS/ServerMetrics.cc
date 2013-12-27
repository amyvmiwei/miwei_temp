/* -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
/// Definitions for ServerMetrics.
/// This file contains definitions for ServerMetrics, a class for
/// aggregating metrics for an individual %RangeServer.

#include <Common/Compat.h>
#include "ServerMetrics.h"

#include <Common/Error.h>
#include <Common/Logger.h>

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace Hypertable::Lib::RS_METRICS;
using namespace std;

ServerMeasurement::ServerMeasurement(const char *measurement, size_t len) {
  memset(this, 0, sizeof(ServerMeasurement));
  parse_measurement(measurement, len);
}

void ServerMeasurement::parse_measurement(const char *measurement, size_t len) {
  vector<String> splits;
  String str(measurement, len);
  boost::split(splits, str, boost::is_any_of(":,"));

  version = atoi(splits[0].c_str());
  if (version == 2) {
    if (splits.size() != 12)
      HT_THROW(Error::PROTOCOL_ERROR,
               format("Measurement string '%s' has %d components, expected 12.",
                      str.c_str(), (int)splits.size()));
  }
  else if (version == 3) {
    if (splits.size() != 14)
      HT_THROW(Error::PROTOCOL_ERROR,
               format("Measurement string '%s' has %d components, expected 14.",
                      str.c_str(), (int)splits.size()));
  }
  else
    HT_THROW(Error::NOT_IMPLEMENTED,
             format("ServerMetrics version=%d expected 2", (int)version));


  timestamp             = strtoll(splits[1].c_str(), 0, 0);
  loadavg               = strtod(splits[2].c_str(), 0);
  disk_bytes_read_rate  = strtod(splits[3].c_str(), 0);
  bytes_written_rate    = strtod(splits[4].c_str(), 0);
  bytes_scanned_rate    = strtod(splits[5].c_str(), 0);
  updates_rate          = strtod(splits[6].c_str(), 0);
  scans_rate            = strtod(splits[7].c_str(), 0);
  cells_written_rate    = strtod(splits[8].c_str(), 0);
  cells_scanned_rate    = strtod(splits[9].c_str(), 0);
  page_in               = strtod(splits[10].c_str(), 0);
  page_out              = strtod(splits[11].c_str(), 0);
  if (version == 3) {
    disk_total          = strtoll(splits[12].c_str(), 0, 0);
    disk_avail          = strtoll(splits[13].c_str(), 0, 0);
  }
}

void ServerMetrics::add_measurement(const char *measurement, size_t len) {
  try {
    ServerMeasurement sm(measurement, len);
    m_measurements.push_back(sm);
  }
  catch (Exception &e) {
    if (e.code() == Error::NOT_IMPLEMENTED) {
      HT_WARN_OUT << e << HT_END;
    }
    else
      HT_THROW(e.code(), e.what());
  }
}


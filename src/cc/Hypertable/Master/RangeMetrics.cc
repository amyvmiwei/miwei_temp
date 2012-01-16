/** -*- c++ -*-
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

#include "Common/Compat.h"

#include <boost/algorithm/string.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Types.h"

#include "RangeMetrics.h"

using namespace Hypertable;
using namespace std;

RangeMeasurement::RangeMeasurement(const char *measurement, size_t len) {
  memset(this, 0, sizeof(RangeMeasurement));
  parse_measurement(measurement, len);
}

void RangeMeasurement::parse_measurement(const char *measurement, size_t len) {
  vector<String> splits;
  String str(measurement, len);
  boost::split(splits, str, boost::is_any_of(":,"));
  version = atoi(splits[0].c_str());

  if (version != 2)
    HT_THROW(Error::NOT_IMPLEMENTED, (String) "ServerMetrics version=" + version
        + " expected 2");

  if (splits.size() != 11)
    HT_THROW(Error::PROTOCOL_ERROR, (String) "Measurement string '" + str
        + "' has " + (int)(splits.size()) + (String)" components, expected 11.");

  timestamp             = strtoll(splits[1].c_str(), 0, 0);
  disk_used             = strtoll(splits[2].c_str(), 0, 0);
  memory_used           = strtoll(splits[3].c_str(), 0, 0);
  disk_byte_read_rate   = strtod(splits[4].c_str(), 0);
  byte_write_rate       = strtod(splits[5].c_str(), 0);
  byte_read_rate        = strtod(splits[6].c_str(), 0);
  update_rate           = strtod(splits[7].c_str(), 0);
  scan_rate             = strtod(splits[8].c_str(), 0);
  cell_write_rate       = strtod(splits[9].c_str(), 0);
  cell_read_rate        = strtod(splits[10].c_str(), 0);
}

RangeMetrics::RangeMetrics(const char *server_id, const char *table_id,
    const char *end_row)
  : m_server_id(server_id), m_table_id(table_id), m_end_row(end_row), m_start_row_set(false),
    m_last_move_set(false) {
}

void RangeMetrics::add_measurement(const char *measurement, size_t len) {
  try {
    RangeMeasurement rm(measurement, len);
    m_measurements.push_back(rm);
  }
  catch (Exception &e) {
    if (e.code() == Error::NOT_IMPLEMENTED) {
      HT_WARN_OUT << e << HT_END;
    }
    else
      HT_THROW(e.code(), e.what());
  }
}

void RangeMetrics::set_last_move(const char *move, size_t len) {
  String str(move, len);
  m_last_move = strtoll(str.c_str(), 0, 0);
  m_last_move_set = true;
}

bool RangeMetrics::is_moveable() const {
  if (!m_start_row_set)
    return false;
  // don't move ROOT range
  if (m_table_id == TableIdentifier::METADATA_ID && m_end_row == Key::END_ROOT_ROW)
    return false;
  // TODO: don't move a range that was moved recently
  return true;
}

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

#include <ctime>

#include "RangeStatsGatherer.h"

using namespace Hypertable;


void RangeStatsGatherer::fetch(RangeDataVector &range_data, TableMutator *mutator, int *log_generation) {
  time_t now = time(0);

  range_data.clear();
  m_table_info_map->get_range_data(range_data, log_generation);
  foreach_ht (RangeData &rd, range_data)
    rd.data = rd.range->get_maintenance_data(range_data.arena(), now, mutator);
}

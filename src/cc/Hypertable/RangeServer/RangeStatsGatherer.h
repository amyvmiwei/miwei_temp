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

#ifndef HYPERTABLE_RANGESTATSGATHERER_H
#define HYPERTABLE_RANGESTATSGATHERER_H

#include "Common/PageArena.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/TableMutator.h"

#include "MaintenanceQueue.h"
#include "Range.h"
#include "TableInfoMap.h"

namespace Hypertable {

  class RangeStatsGatherer : public ReferenceCount {
  public:
    RangeStatsGatherer(TableInfoMapPtr &table_info_map) : m_table_info_map(table_info_map) { }

    virtual ~RangeStatsGatherer() { }

    void fetch(RangeDataVector &range_data, TableMutator *mutator=0, int *log_generation=0);

  private:
    TableInfoMapPtr m_table_info_map;
  };
  typedef intrusive_ptr<RangeStatsGatherer> RangeStatsGathererPtr;

}

#endif // HYPERTABLE_RANGESTATSGATHERER_H



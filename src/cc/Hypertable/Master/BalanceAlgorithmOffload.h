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

#ifndef HYPERTABLE_BALANCEALGORITHMOFFLOAD_H
#define HYPERTABLE_BALANCEALGORITHMOFFLOAD_H

#include <vector>
#include <set>

#include "BalanceAlgorithm.h"
#include "Context.h"

namespace Hypertable {

  class BalanceAlgorithmOffload : public BalanceAlgorithm {

  public:
    BalanceAlgorithmOffload(ContextPtr &context,
                            std::vector<RangeServerStatistics> &statistics,
                            String arguments);

    virtual void compute_plan(BalancePlanPtr &plan,
                              std::vector<RangeServerConnectionPtr> &balanced);

  private:
    ContextPtr m_context;
    std::vector<RangeServerStatistics> m_statistics;
    std::set<String> m_offload_servers;
  };


}

#endif // HYPERTABLE_BALANCEALGORITHMOFFLOAD_H

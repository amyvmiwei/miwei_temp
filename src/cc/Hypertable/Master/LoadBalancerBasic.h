/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

#ifndef HYPERTABLE_LOADBALANCERBASIC_H
#define HYPERTABLE_LOADBALANCERBASIC_H

#include "LoadBalancer.h"

namespace Hypertable {

  class LoadBalancerBasic : public LoadBalancer {
  public:

    LoadBalancerBasic(ContextPtr context) : LoadBalancer(context) { }

    //void receive_monitoring_data(vector<RangeServerStatistics> &stats) = 0;
    //void balance() = 0;

    //String assign_to_server(TableIdentifier &tid, RangeIdentifier &rid) = 0;
    //void range_move_loaded(TableIdentifier &tid, RangeIdentifier &rid) = 0;
    //void range_relinquish_acknowledged(TableIdentifier &tid, RangeIdentifier &rid) = 0;
    //time_t maintenance_interval() = 0;
  };

} // namespace Hypertable

#endif // HYPERTABLE_LOADBALANCERBASIC_H

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

#ifndef HYPERTABLE_LOADBALANCER_H
#define HYPERTABLE_LOADBALANCER_H

#include <set>

#include <boost/thread/condition.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/BalancePlan.h"

#include "RSMetrics.h"
#include "Context.h"

namespace Hypertable {
  using namespace boost::posix_time;

  class OperationBalance;

  class LoadBalancer : public ReferenceCount {
  public:
    virtual void balance(const String &algorithm=String()) = 0;
    virtual void transfer_monitoring_data(vector<RangeServerStatistics> &stats)=0;

    LoadBalancer(ContextPtr context) : m_context(context), m_last_balance_time(min_date_time) {
      m_balance_interval     = m_context->props->get_i32("Hypertable.LoadBalancer.Interval");
      m_balance_window_start = duration_from_string(m_context->props->get_str(
          "Hypertable.LoadBalancer.WindowStart"));
      m_balance_window_end   = duration_from_string(m_context->props->get_str(
          "Hypertable.LoadBalancer.WindowEnd"));
      m_balance_wait = m_context->props->get_i32("Hypertable.LoadBalancer.ServerWaitInterval");
      m_balance_loadavg_threshold = m_context->props->get_f64("Hypertable.LoadBalancer.LoadavgThreshold");

    }

    virtual bool has_plan_moves() {
      ScopedLock lock(m_mutex);
      if (!m_plan)
        return false;
      else
        return (m_plan->moves.size()>0);
    }

    virtual void register_plan(BalancePlanPtr &plan);
    virtual bool get_destination(const TableIdentifier &table, const RangeSpec &range, String &location);
    virtual bool move_complete(const TableIdentifier &table, const RangeSpec &range, int32_t error=0);
    virtual bool wait_for_complete(RangeMoveSpecPtr &move, uint32_t timeout_millis);

    virtual void set_balanced();
    virtual void get_root_location(String &location);
    //String assign_to_server(TableIdentifier &tid, RangeIdentifier &rid) = 0;
    //void range_move_loaded(TableIdentifier &tid, RangeIdentifier &rid) = 0;
    //void range_relinquish_acknowledged(TableIdentifier &tid, RangeIdentifier &rid) = 0;
    //time_t maintenance_interval() = 0;

  protected:
    Mutex m_mutex;
    boost::condition m_cond;
    ContextPtr m_context;
    uint32_t m_balance_interval;
    uint32_t m_balance_wait;
    time_duration m_balance_window_start;
    time_duration m_balance_window_end;
    ptime m_last_balance_time;
    double m_balance_loadavg_threshold;
    std::vector <RangeServerConnectionPtr> m_unbalanced_servers;
  private:
    struct lt_move_spec {
      bool operator()(const RangeMoveSpecPtr &ms1, const RangeMoveSpecPtr &ms2) const  {
        if (ms1->table < ms2->table)
          return true;
        else if (ms1->table == ms2->table) {
          if (ms1->range < ms2->range)
            return true;
        }
        return false;
      }
    };
    typedef std::set<RangeMoveSpecPtr, lt_move_spec> MoveSetT;

    BalancePlanPtr m_plan;

    MoveSetT m_current_set;
    MoveSetT m_incomplete_set;
  };
  typedef intrusive_ptr<LoadBalancer> LoadBalancerPtr;

} // namespace Hypertable

#endif // HYPERTABLE_LOADBALANCER_H

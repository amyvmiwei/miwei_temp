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
#include "Common/Sweetener.h"
#include "Common/Time.h"

#include "LoadBalancer.h"
#include "OperationBalance.h"
#include "Utility.h"

using namespace Hypertable;

void LoadBalancer::register_plan(BalancePlanPtr &plan) {
  ScopedLock lock(m_mutex);

  m_plan = plan;

  // Insert moves into current set
  foreach_ht (RangeMoveSpecPtr &move, m_plan->moves)
    m_current_set.insert(move);

  HT_INFO_OUT << "Balance plan registered move " << m_plan->moves.size() << " ranges" <<
      ", BalancePlan = " << *m_plan<< HT_END;
}

bool LoadBalancer::get_destination(const TableIdentifier &table, const RangeSpec &range, String &location) {
  ScopedLock lock(m_mutex);
  RangeMoveSpecPtr move_spec = new RangeMoveSpec();

  move_spec->table = table;
  move_spec->table.generation = 0;
  move_spec->range = range;

  MoveSetT::iterator iter;

  if ((iter = m_current_set.find(move_spec)) != m_current_set.end())
    location = (*iter)->dest_location;
  else if ((iter = m_incomplete_set.find(move_spec)) != m_incomplete_set.end())
    location = (*iter)->dest_location;
  else {
    if (!Utility::next_available_server(m_context, location))
      return false;
  }
  return true;
}


bool LoadBalancer::move_complete(const TableIdentifier &table, const RangeSpec &range, int32_t error) {
  ScopedLock lock(m_mutex);
  RangeMoveSpecPtr move_spec = new RangeMoveSpec();

  move_spec->table = table;
  move_spec->table.generation = 0;
  move_spec->range = range;

  MoveSetT::iterator iter;

  if ((iter = m_current_set.find(move_spec)) != m_current_set.end()) {
    (*iter)->complete = true;
    (*iter)->error = error;
    m_cond.notify_all();
  }
  else if ((iter = m_incomplete_set.find(move_spec)) != m_incomplete_set.end()) {
    (*iter)->complete = true;
    (*iter)->error = error;
    m_cond.notify_all();
  }

  return true;
}


bool LoadBalancer::wait_for_complete(RangeMoveSpecPtr &move, uint32_t timeout_millis) {
  ScopedLock lock(m_mutex);
  boost::xtime expire_time;
  MoveSetT::iterator iter;

  boost::xtime_get(&expire_time, boost::TIME_UTC_);
  expire_time.sec += timeout_millis/1000;
  expire_time.nsec += (int32_t)((double)(timeout_millis-(timeout_millis/1000)) * 1000000.0);

  while ((iter = m_current_set.find(move)) != m_current_set.end() &&
         (*iter)->complete == false) {
    if (!m_cond.timed_wait(lock, expire_time))
      return false;
  }

  return true;
}

void LoadBalancer::set_balanced() {
    m_context->set_servers_balanced(m_unbalanced_servers);
}

void LoadBalancer::get_root_location(String &location) {
  location = Utility::root_range_location(m_context);
}

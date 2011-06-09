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
#include "Common/Compat.h"
#include "Common/Sweetener.h"

#include "LoadBalancer.h"
#include "OperationBalance.h"
#include "Utility.h"

using namespace Hypertable;

void LoadBalancer::register_plan(BalancePlanPtr &plan) {
  ScopedLock lock(m_mutex);

  if (m_plan)
    HT_THROW(Error::MASTER_OPERATION_IN_PROGRESS, "Balance plan already registered");

  m_plan = plan;

  // Insert moves into current set
  foreach (RangeMoveSpecPtr &move, m_plan->moves) {
    std::pair<MoveSetT::iterator, bool> ret = m_current_set.insert(move);
    HT_ASSERT(ret.second);
  }
  
}


void LoadBalancer::deregister_plan(BalancePlanPtr &plan) {
  ScopedLock lock(m_mutex);

  foreach (RangeMoveSpecPtr &move, m_plan->moves) {
    if (!move->complete) {
      std::pair<MoveSetT::iterator, bool> ret = m_incomplete_set.insert(move);
      if (!ret.second) {
        m_incomplete_set.erase(ret.first);
        std::pair<MoveSetT::iterator, bool> ret = m_incomplete_set.insert(move);
      }
    }
  }

  m_current_set.clear();
  m_plan = 0;
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

  boost::xtime_get(&expire_time, boost::TIME_UTC);
  expire_time.sec += timeout_millis/1000;
  expire_time.nsec += (double)(timeout_millis-(timeout_millis/1000)) * 1000000.0;

  while ((iter = m_current_set.find(move)) != m_current_set.end() &&
         (*iter)->complete == false) {
    if (!m_cond.timed_wait(lock, expire_time))
      return false;
  }

  return true;
}

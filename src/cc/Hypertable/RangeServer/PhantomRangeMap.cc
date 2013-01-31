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

#include "Hypertable/Lib/Types.h"

#include "PhantomRangeMap.h"

using namespace std;
using namespace Hypertable;


PhantomRangeMap::PhantomRangeMap(int plan_generation) : 
  m_plan_generation(plan_generation), m_state(0) {
  m_tableinfo_map = new TableInfoMap();
}

void PhantomRangeMap::reset(int plan_generation) {
  m_plan_generation = plan_generation;
  m_state = 0;
  for (Map::iterator iter = m_map.begin(); iter != m_map.end(); ++iter)
    iter->second->purge_incomplete_fragments();
}


void PhantomRangeMap::insert(const QualifiedRangeSpec &spec,
                             const RangeState &state, SchemaPtr &schema,
                             const vector<uint32_t> &fragments) {
  if (m_map.find(spec) == m_map.end()) {
    QualifiedRangeSpec copied_spec(m_arena, spec);
    RangeState copied_state(m_arena, state);
    m_map[copied_spec] =
      new PhantomRange(copied_spec, copied_state, schema, fragments);
  }
}

void PhantomRangeMap::get(const QualifiedRangeSpec &spec, PhantomRangePtr &phantom_range) {
  Map::iterator it = m_map.find(spec);
  if (it == m_map.end())
    phantom_range = 0;
  else
    phantom_range = it->second;
}

bool PhantomRangeMap::initial() const {
  return m_state == 0;
}

void PhantomRangeMap::set_loaded() {
  HT_ASSERT((m_state & PhantomRange::LOADED) == 0);
  m_state |= PhantomRange::LOADED;
}

bool PhantomRangeMap::loaded() const {
  return (m_state & PhantomRange::LOADED) == PhantomRange::LOADED;
}


bool PhantomRangeMap::replayed() const {
  return (m_state & PhantomRange::REPLAYED) == PhantomRange::REPLAYED;
}

void PhantomRangeMap::set_prepared() {
  HT_ASSERT((m_state & PhantomRange::PREPARED) == 0);
  m_state |= PhantomRange::PREPARED;
}

bool PhantomRangeMap::prepared() const {
  return (m_state & PhantomRange::PREPARED) == PhantomRange::PREPARED;
}

void PhantomRangeMap::set_committed() {
  HT_ASSERT((m_state & PhantomRange::COMMITTED) == 0);
  m_state |= PhantomRange::COMMITTED;
}

bool PhantomRangeMap::committed() const {
  return (m_state & PhantomRange::COMMITTED) == PhantomRange::COMMITTED;
}

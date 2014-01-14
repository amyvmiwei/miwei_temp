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
#include "Common/Config.h"

#include "GroupCommit.h"
#include "RangeServer.h"


using namespace Hypertable;
using namespace Hypertable::Config;

GroupCommit::GroupCommit(RangeServer *range_server) : m_range_server(range_server), m_counter(0) {

  m_commit_interval = get_i32("Hypertable.RangeServer.CommitInterval");

}


void
GroupCommit::add(EventPtr &event, uint64_t cluster_id, SchemaPtr &schema,
                 const TableIdentifier *table, uint32_t count,
                 StaticBuffer &buffer, uint32_t flags) {
  ScopedLock lock(m_mutex);
  TableUpdateMap::iterator iter;
  ClientUpdateRequest *request = new ClientUpdateRequest();
  boost::xtime expire_time = event->expiration_time();
  ClusterTableIdPair key = std::make_pair(cluster_id, *table);

  key.second.id = m_flyweight_strings.get(table->id);

  request->buffer = buffer;
  request->count = count;
  request->event = event;

  if ((iter = m_table_map.find(key)) == m_table_map.end()) {

    TableUpdate *tu = new TableUpdate();
    tu->cluster_id = cluster_id;
    tu->id = key.second;
    tu->commit_interval = schema->get_group_commit_interval();
    tu->commit_iteration = (tu->commit_interval+(m_commit_interval-1)) / m_commit_interval;
    tu->total_count = count;
    tu->total_buffer_size = buffer.size;
    tu->expire_time = expire_time;
    tu->requests.push_back(request);

    m_table_map[key] = tu;
    return;
  }

  if (expire_time.sec > (*iter).second->expire_time.sec)
    (*iter).second->expire_time = expire_time;
  (*iter).second->total_count += count;
  (*iter).second->total_buffer_size += buffer.size;
  (*iter).second->requests.push_back(request);
}



void GroupCommit::trigger() {
  ScopedLock lock(m_mutex);
  std::vector<TableUpdate *> updates;
  boost::xtime expire_time;

  // Clear to Jan 1, 1970
  memset(&expire_time, 0, sizeof(expire_time));

  m_counter++;

  TableUpdateMap::iterator iter = m_table_map.begin();
  while (iter != m_table_map.end()) {
    if ((m_counter % (*iter).second->commit_iteration) == 0) {
      TableUpdateMap::iterator remove_iter = iter;
      if (iter->second->expire_time.sec > expire_time.sec)
	expire_time = iter->second->expire_time;
      ++iter;
      updates.push_back((*remove_iter).second);
      m_table_map.erase(remove_iter);
    }
    else
      ++iter;
  }

  if (!updates.empty())
    m_range_server->batch_update(updates, expire_time);

}

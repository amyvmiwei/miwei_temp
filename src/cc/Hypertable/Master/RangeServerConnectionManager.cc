/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#include <Common/Compat.h>

#include "RangeServerConnectionManager.h"

#include <Common/Config.h>

#include <algorithm>
#include <iterator>

using namespace Hypertable;
using namespace std;

RangeServerConnectionManager::RangeServerConnectionManager() {
  m_connections_iter = m_connections.end();
  m_disk_threshold = Config::properties->get_i32("Hypertable.Master.DiskThreshold.Percentage");
}

void RangeServerConnectionManager::add_server(RangeServerConnectionPtr &rsc) {
  lock_guard<mutex> lock(m_mutex);

  {
    RangeServerConnectionPtr tmp_rsc;
    if (find_server_by_location_unlocked(rsc->location(), tmp_rsc))
      HT_FATALF("Attempt to add %s which conflicts with previously added %s",
                rsc->to_str().c_str(), tmp_rsc->to_str().c_str());
  }

  auto result = m_connections.push_back( RangeServerConnectionEntry(rsc) );
  if (!result.second)
    HT_FATALF("Attempt to add %s which conflicts with previously added entry",
              rsc->to_str().c_str());
}

bool
RangeServerConnectionManager::remove_server(const string &location,
                                            RangeServerConnectionPtr &rsc) {
  lock_guard<mutex> lock(m_mutex);

  LocationIndex &hash_index = m_connections.get<1>();
  auto iter = hash_index.find(location);
  if (iter != hash_index.end()) {
    rsc = iter->rsc;
    rsc->set_removed();
    hash_index.erase(iter);
    m_connections_iter = m_connections.begin();
    return true;
  }
  return false;
}

bool RangeServerConnectionManager::connect_server(RangeServerConnectionPtr &rsc,
        const String &hostname, InetAddr local_addr, InetAddr public_addr) {
  lock_guard<mutex> lock(m_mutex);
  LocationIndex &location_index = m_connections.get<1>();
  LocationIndex::iterator orig_iter;

  HT_INFOF("connect_server(%s, '%s', local=%s, public=%s)",
           rsc->location().c_str(), hostname.c_str(),
           local_addr.format().c_str(), public_addr.format().c_str());

  if ((orig_iter = location_index.find(rsc->location())) == location_index.end()) {

    if (rsc->connect(hostname, local_addr, public_addr))
      m_conn_count++;

    m_connections.push_back(RangeServerConnectionEntry(rsc));
  }
  else {
    bool needs_reindex = false;

    rsc = orig_iter->rsc;

    if (rsc->connected()) {
      HT_ERRORF("Attempted to connect '%s' but failed because already connected.",
                rsc->location().c_str());
      return false;
    }

    if (hostname != rsc->hostname()) {
      HT_INFOF("Changing hostname for %s from '%s' to '%s'",
               rsc->location().c_str(), rsc->hostname().c_str(),
               hostname.c_str());
      needs_reindex = true;
    }

    if (local_addr != rsc->local_addr()) {
      HT_INFOF("Changing local address for %s from '%s' to '%s'",
               rsc->location().c_str(), rsc->local_addr().format().c_str(),
               local_addr.format().c_str());
      needs_reindex = true;
    }

    if (public_addr != rsc->public_addr()) {
      HT_INFOF("Changing public address for %s from '%s' to '%s'",
               rsc->location().c_str(), rsc->public_addr().format().c_str(),
               public_addr.format().c_str());
      needs_reindex = true;
    }

    if (orig_iter->rsc->connect(hostname, local_addr, public_addr))
      m_conn_count++;

    if (needs_reindex) {
      location_index.erase(orig_iter);
      m_connections.push_back(RangeServerConnectionEntry(rsc));
      m_connections_iter = m_connections.begin();
    }
  }
  return true;
}

bool RangeServerConnectionManager::disconnect_server(RangeServerConnectionPtr &rsc) {
  lock_guard<mutex> lock(m_mutex);
  if (rsc->disconnect()) {
    HT_ASSERT(m_conn_count > 0);
    m_conn_count--;
    return true;
  }
  return false;
}

bool RangeServerConnectionManager::is_connected(const String &location) {
  lock_guard<mutex> lock(m_mutex);
  LocationIndex &location_index = m_connections.get<1>();
  LocationIndex::iterator iter = location_index.find(location);
  if (iter != location_index.end() && iter->rsc->connected())
    return true;
  return false;
}

bool RangeServerConnectionManager::find_server_by_location(const String &location,
        RangeServerConnectionPtr &rsc) {
  lock_guard<mutex> lock(m_mutex);
  return find_server_by_location_unlocked(location, rsc);
}

bool RangeServerConnectionManager::find_server_by_location_unlocked(const String &location, RangeServerConnectionPtr &rsc) {
  LocationIndex &hash_index = m_connections.get<1>();
  LocationIndex::iterator lookup_iter;

  if ((lookup_iter = hash_index.find(location)) == hash_index.end()) {
    //HT_DEBUG_OUT << "can't find server with location=" << location << HT_END;
    //for (Sequence::iterator iter = m_connections.begin(); iter != m_connections.end(); ++iter) {
    //  HT_DEBUGF("Contains %s host=%s local=%s public=%s", iter->location().c_str(),
    //           iter->hostname().c_str(), iter->local_addr().format().c_str(),
    //           iter->public_addr().format().c_str());
    //}
    rsc = 0;
    return false;
  }
  rsc = lookup_iter->rsc;
  return true;
}


bool RangeServerConnectionManager::find_server_by_hostname(const String &hostname,
        RangeServerConnectionPtr &rsc) {
  lock_guard<mutex> lock(m_mutex);
  HostnameIndex &hash_index = m_connections.get<2>();

  pair<HostnameIndex::iterator, HostnameIndex::iterator> p
      = hash_index.equal_range(hostname);
  if (p.first != p.second) {
    rsc = p.first->rsc;
    if (++p.first == p.second)
      return true;
    rsc = 0;
  }
  return false;
}

bool RangeServerConnectionManager::find_server_by_public_addr(InetAddr addr,
        RangeServerConnectionPtr &rsc) {
  lock_guard<mutex> lock(m_mutex);
  PublicAddrIndex &hash_index = m_connections.get<3>();
  PublicAddrIndex::iterator lookup_iter;

  if ((lookup_iter = hash_index.find(addr)) == hash_index.end()) {
    rsc = 0;
    return false;
  }
  rsc = lookup_iter->rsc;
  return true;
}

bool RangeServerConnectionManager::find_server_by_local_addr(InetAddr addr,
        RangeServerConnectionPtr &rsc) {
  lock_guard<mutex> lock(m_mutex);
  LocalAddrIndex &hash_index = m_connections.get<4>();

  for (pair<LocalAddrIndex::iterator, LocalAddrIndex::iterator> p
          = hash_index.equal_range(addr);
       p.first != p.second; ++p.first) {
    if (p.first->connected()) {
      rsc = p.first->rsc;
      return true;
    }
  }
  return false;
}


bool RangeServerConnectionManager::next_available_server(RangeServerConnectionPtr &rsc,
                                                         bool urgent) {
  lock_guard<mutex> lock(m_mutex);
  double minimum_disk_use = 100;
  RangeServerConnectionPtr urgent_server;

  if (m_connections.empty())
    return false;

  if (m_connections_iter == m_connections.end())
    m_connections_iter = m_connections.begin();

  auto saved_iter = m_connections_iter;

  do {
    ++m_connections_iter;
    if (m_connections_iter == m_connections.end())
      m_connections_iter = m_connections.begin();
    if (m_connections_iter->rsc->connected() &&
        !m_connections_iter->rsc->is_recovering()) {
      if (m_connections_iter->rsc->get_disk_fill_percentage() <
          (double)m_disk_threshold) {
        rsc = m_connections_iter->rsc;
        return true;
      }
      if (m_connections_iter->rsc->get_disk_fill_percentage() < minimum_disk_use) {
        minimum_disk_use = m_connections_iter->rsc->get_disk_fill_percentage();
        urgent_server = m_connections_iter->rsc;
      }
    }
  } while (m_connections_iter != saved_iter);

  if (urgent && urgent_server) {
    rsc = urgent_server;
    return true;
  }

  return false;
}

size_t RangeServerConnectionManager::server_count() {
  lock_guard<mutex> lock(m_mutex);
  size_t count = 0;
  for (auto &entry : m_connections) {
    if (!entry.rsc->get_removed())
      count++;
  }
  return count;
}


void RangeServerConnectionManager::get_servers(vector<RangeServerConnectionPtr> &servers) {
  lock_guard<mutex> lock(m_mutex);
  for (auto &entry : m_connections) {
    if (!entry.rsc->get_removed())
      servers.push_back(entry.rsc);
  }
}

void RangeServerConnectionManager::get_valid_connections(StringSet &locations,
                                 vector<RangeServerConnectionPtr> &connections) {
  lock_guard<mutex> lock(m_mutex);
  LocationIndex &location_index = m_connections.get<1>();
  LocationIndex::iterator iter;

  for (auto &location : locations) {
    auto iter = location_index.find(location);
    if (iter != location_index.end() && !iter->rsc->is_recovering())
      connections.push_back(iter->rsc);
  }
}

void RangeServerConnectionManager::set_servers_balanced(const vector<RangeServerConnectionPtr> &unbalanced) {
  lock_guard<mutex> lock(m_mutex);
  foreach_ht (const RangeServerConnectionPtr rsc, unbalanced) {
    rsc->set_balanced();
  }
}

bool RangeServerConnectionManager::exist_unbalanced_servers() {
  lock_guard<mutex> lock(m_mutex);
  for (auto &entry : m_connections) {
    if (!entry.rsc->get_removed() && !entry.rsc->is_recovering() && !entry.rsc->get_balanced())
      return true;
  }
  return false;
}

void RangeServerConnectionManager::set_range_server_state(vector<RangeServerState> &states) {
  lock_guard<mutex> lock(m_mutex);
  LocationIndex &hash_index = m_connections.get<1>();
  LocationIndex::iterator lookup_iter;

  for (auto &state : states) {
    if ((lookup_iter = hash_index.find(state.location)) == hash_index.end())
      continue;
    lookup_iter->rsc->set_disk_fill_percentage(state.disk_usage);
  }
}

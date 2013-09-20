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

#ifndef HYPERTABLE_RANGESERVERCONNECTIONMANAGER_H
#define HYPERTABLE_RANGESERVERCONNECTIONMANAGER_H

#include "Common/HashMap.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"

#include "AsyncComm/Comm.h"

#include "RangeServerConnection.h"

#include <set>
#include <utility>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>


namespace Hypertable {

  using namespace boost::multi_index;

  struct RangeServerState {
    String location;
    double disk_usage;
  };

  class RangeServerConnectionManager : public ReferenceCount {
  public:
    RangeServerConnectionManager();
    void add_server(RangeServerConnectionPtr &rsc);
    bool connect_server(RangeServerConnectionPtr &rsc, const String &hostname, InetAddr local_addr, InetAddr public_addr);
    bool disconnect_server(RangeServerConnectionPtr &rsc);
    bool is_connected(const String &location);
    void wait_for_server();
    void erase_server(RangeServerConnectionPtr &rsc);
    bool find_server_by_location(const String &location, RangeServerConnectionPtr &rsc);
    bool find_server_by_hostname(const String &hostname, RangeServerConnectionPtr &rsc);
    bool find_server_by_public_addr(InetAddr addr, RangeServerConnectionPtr &rsc);
    bool find_server_by_local_addr(InetAddr addr, RangeServerConnectionPtr &rsc);
    bool next_available_server(RangeServerConnectionPtr &rsc, bool urgent=false);
    void get_unbalanced_servers(StringSet &locations,
                                std::vector<RangeServerConnectionPtr> &unbalanced);
    void set_servers_balanced(const std::vector<RangeServerConnectionPtr> &servers);
    bool exist_unbalanced_servers();
    size_t server_count();
    size_t connected_server_count();
    void get_servers(std::vector<RangeServerConnectionPtr> &servers);
    void get_valid_connections(StringSet &locations,
                               std::vector<RangeServerConnectionPtr> &connections);
    void get_connected_servers(StringSet &locations);
    size_t connection_count() { ScopedLock lock(mutex); return m_conn_count; }
    void set_range_server_state(std::vector<RangeServerState> &states);

  private:

    bool find_server_by_location_unlocked(const String &location, RangeServerConnectionPtr &rsc);

    typedef std::map<String, uint64_t> HandleMap;

    class RangeServerConnectionEntry {
    public:
      RangeServerConnectionEntry(RangeServerConnectionPtr &_rsc) : rsc(_rsc) { }
      RangeServerConnectionPtr rsc;
      String location() const { return rsc->location(); }
      String hostname() const { return rsc->hostname(); }
      InetAddr public_addr() const { return rsc->public_addr(); }
      InetAddr local_addr() const { return rsc->local_addr(); }
      bool connected() const { return rsc->connected(); }
      bool removed() const { return rsc->get_removed(); }
    };

    struct InetAddrHash {
      std::size_t operator()(InetAddr addr) const {
        return (std::size_t)(addr.sin_addr.s_addr ^ addr.sin_port);
      }
    };

    typedef boost::multi_index_container<
      RangeServerConnectionEntry,
      indexed_by<
        sequenced<>,
        hashed_unique<const_mem_fun<RangeServerConnectionEntry, String,
            &RangeServerConnectionEntry::location> >,
        hashed_non_unique<const_mem_fun<RangeServerConnectionEntry, String,
            &RangeServerConnectionEntry::hostname> >,
        hashed_non_unique<const_mem_fun<RangeServerConnectionEntry, InetAddr,
            &RangeServerConnectionEntry::public_addr>, InetAddrHash>,
        hashed_non_unique<const_mem_fun<RangeServerConnectionEntry, InetAddr,
            &RangeServerConnectionEntry::local_addr>, InetAddrHash>
        >
      > ServerList;

    typedef ServerList::nth_index<0>::type Sequence;
    typedef ServerList::nth_index<1>::type LocationIndex;
    typedef ServerList::nth_index<2>::type HostnameIndex;
    typedef ServerList::nth_index<3>::type PublicAddrIndex;
    typedef ServerList::nth_index<4>::type LocalAddrIndex;

    Mutex mutex;
    boost::condition cond;
    Comm *comm;
    ServerList m_server_list;
    ServerList::iterator m_server_list_iter;
    size_t m_conn_count;
    int32_t m_disk_threshold;
  };
  typedef intrusive_ptr<RangeServerConnectionManager> RangeServerConnectionManagerPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVERCONNECTIONMANAGER_H

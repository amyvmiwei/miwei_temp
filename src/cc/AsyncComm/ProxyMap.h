/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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

#ifndef HYPERTABLE_PROXYMAP_H
#define HYPERTABLE_PROXYMAP_H

#include "Common/InetAddr.h"
#include "Common/HashMap.h"
#include "Common/Mutex.h"
#include "Common/SockAddrMap.h"
#include "Common/String.h"

#include "CommBuf.h"

namespace Hypertable {

  class ProxyAddressInfo {
  public:
    ProxyAddressInfo() { }
    ProxyAddressInfo(const String &h, InetAddr a) : hostname(h), addr(a) { }
    String hostname;
    InetAddr addr;
  } ;

  typedef hash_map<String, ProxyAddressInfo> ProxyMapT;

  /**
   * ProxyMap is a class that maintains mappings from proxy name to IP address.
   * Hypertable uses <i>proxy names</i> (e.g. "rs1") to refer to servers so
   * that the system can continue to operate properly even when servers are
   * reassigned IP addresses, such as starting and stopping Hypertable running
   * on EBS volumes in AWS EC2.  There is a single ProxyMap associated with
   * each Comm layer and one of the connected participants is designated as
   * the <i>proxy master</i> by setting the global variable
   * <code>ReactorFactory::proxy_master</code> to <i>true</i>.
   * In Hypertable, the Master is designated as the proxy master.  The proxy
   * master is responsible for assigning proxy names which are just mnemonic
   * strings (e.g. "rs1").  Whenever a server connects to the proxy master,
   * the proxy master will either assign the newly connected server a proxy
   * name or obtain it via a handshake and should then update the proxy map with
   * the <i>{proxy name, IP address}</i> association and will propagate the new
   * proxy map information to all connected participants.  Once this is
   * complete, all connected participants and send and receive messages to any
   * participant using its proxy name.  The CommAddress class is an abstraction
   * that can hold either a proxy name or an IP address and used to identify
   * the destination of a message.
   *
   */
  class ProxyMap {

  public:
    
    void update_mapping(const String &proxy, const String &hostname, const InetAddr &addr,
			ProxyMapT &invalidated_map, ProxyMapT &new_map);
    void update_mappings(String &mappings, ProxyMapT &invalidated_map,
			 ProxyMapT &new_map);
    bool get_mapping(const String &proxy, String &hostname, InetAddr &addr);

    String get_proxy(InetAddr &addr);
    
    void get_map(ProxyMapT &map) {
      map = m_map;
    }

    CommBuf *create_update_message();

  private:

    void invalidate(const String &proxy, const InetAddr &addr,
		    ProxyMapT &invalidated_mappings);

    /// 
    Mutex     m_mutex;
    ProxyMapT m_map;
    SockAddrMap<String> m_reverse_map;
  };

}

#endif // HYPERTABLE_PROXYMAP_H

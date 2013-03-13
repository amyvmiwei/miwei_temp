/*
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

/** @file
 * Declarations for ProxyMap.
 * This file contains type declarations for ProxyMap, a class that provides
 * a mapping from a set of proxy names (e.g. "rs1") to their associated IP
 * addresses.
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

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Holds address and hostname associated with a proxy name.
   */
  class ProxyAddressInfo {
  public:
    ProxyAddressInfo() { }
    ProxyAddressInfo(const String &h, InetAddr a) : hostname(h), addr(a) { }
    String hostname;
    InetAddr addr;
  };

  /// Forward mapping hash type from proxy name to ProxyAddressInfo
  typedef hash_map<String, ProxyAddressInfo> ProxyMapT;

  /** Maps a set of proxy names to their associated IP addresses.
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
   */
  class ProxyMap {

  public:

    /** Updates a proxy name mapping.  This method first checks to see if there
     * is already a mapping from <code>proxy</code> to <code>addr</code> in the
     * forawrd map.  If so, then it updates the hostname.  Otherwise, it
     * invalidates <code>proxy</code> from the forward map and <code>addr</code>
     * from the reverse map and adds entries to the forward and reverse maps
     * for the new mapping.  Any invalidated mappings are added to
     * <code>invalidated_map</code> and the new forward mapping from
     * <code>proxy</code> to ProxyAddressInfo is added to <code>new_map</code>.
     * @param proxy Proxy name of new/updated mapping
     * @param hostname Hostname of new/updated mapping
     * @param addr InetAddr of new/updated mapping
     * @param invalidated_map Reference to return map to hold invalidated
     *        mappings
     * @param new_map Reference to return map to hold new forward mapping
     */
    void update_mapping(const String &proxy, const String &hostname,
                        const InetAddr &addr,ProxyMapT &invalidated_map,
                        ProxyMapT &new_map);

    /** Update mappings from proxy map update message string.
     * One process in the system is designated as the proxy master and is
     * responsible for updating the proxy mappings for all connected
     * processes.  This method is called by connected processes to update
     * their proxy maps from a proxy map update message received by the
     * proxy master.  The proxy map update message consists of a list of
     * mappings in the following format:
     * @verbatim <proxy> '\t' <hostname> '\t' <addr> '\n' @endverbatim
     * For each mapping in <code>mappings</code> this method first checks to see
     * if there is already a mapping from <code>proxy</code> to
     * <code>addr</code> in the forawrd map.  If so, then it updates the
     * hostname.  Otherwise, it invalidates <code>proxy</code> from the forward
     * map and <code>addr</code> from the reverse map and adds entries to the
     * forward and reverse maps for the new mapping.  Any invalidated mappings
     * are added to <code>invalidated_map</code> and the new forward mapping
     * from <code>proxy</code> to ProxyAddressInfo is added to
     * <code>new_map</code>.
     * @param mappings Proxy map update message string
     * @param invalidated_map Reference to return map to hold invalidated
     *        mappings
     * @param new_map Reference to return map to hold new forward mapping
     */
    void update_mappings(String &mappings, ProxyMapT &invalidated_map,
			 ProxyMapT &new_map);

    /** Returns proxy map data for <code>proxy</code>.
     * This method looks up <code>proxy</code> in the forward map and returns
     * the associated hostname and address information, if found.
     * @param proxy Proxy name for which to fetch mapping information
     * @param hostname Reference to returned hostname
     * @param addr Reference to returned address
     * @return <i>true</i> if mapping found, <i>false</i> otherwise.
     */
    bool get_mapping(const String &proxy, String &hostname, InetAddr &addr);

    /** Returns proxy name for <code>addr</code>.  This method looks up
     * <code>addr</code> in the reverse map and returns the proxy name,
     * if found.
     * @param addr Address for which to fetch proxy name
     * @return Proxy name of <code>addr</code> if found in reverse map,
     * otherwise the empty string.
     */
    String get_proxy(InetAddr &addr);
    
    /** Returns the forward map (proxy name to ProxyAddressInfo)
     * @param map Reference to return forward map
     */
    void get_map(ProxyMapT &map) {
      map = m_forward_map;
    }

    /** Creates a proxy map update message.  This method is called by the proxy
     * master to create a proxy map update message to be sent to all connected
     * processes.  The proxy map update message consists of a list of proxy
     * mappings in the following format:
     * @verbatim <proxy> '\t' <hostname> '\t' <addr> '\n' @endverbatim
     * The forward map is traversed to generate the list of mappings which are
     * added to a newly allocated CommBuf object.  The CommBuf object is
     * initialized with a CommHeader that has the
     * CommHeader::FLAGS_BIT_PROXY_MAP_UPDATE bit set in its flags member.
     * @return CommBuf object holding the proxy map update message (to be freed
     * by caller).
     */
    CommBuf *create_update_message();

  private:

    /** Invalidates (removes) mapping from forward and reverse maps.
     * This method looks up <code>proxy</code> in the forward map and removes
     * it if it exists.  It also looks up <code>addr</code> in the reverse map
     * and removes it if it exists.  The removed mappings are added to
     * <code>invalidated_mappings</code>.
     * @param proxy Proxy name to invalidate
     * @param addr IP address to invalidate
     * @param invalidated_mappings Reference to ProxyMapT object to hold
     * invalidated mappings.
     */
    void invalidate(const String &proxy, const InetAddr &addr,
		    ProxyMapT &invalidated_mappings);

    /// %Mutex for serializing concurrent access
    Mutex m_mutex;

    /// Forward map from proxy name to ProxyAddressInfo
    ProxyMapT m_forward_map;

    /// Reverse map from IP address to proxy name
    SockAddrMap<String> m_reverse_map;
  };

  /** @}*/
}

#endif // HYPERTABLE_PROXYMAP_H

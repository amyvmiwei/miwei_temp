/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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

#ifndef HYPERTABLE_HANDLERMAP_H
#define HYPERTABLE_HANDLERMAP_H

#include <cassert>

//#define HT_DISABLE_LOG_DEBUG

#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"
#include "Common/Time.h"
#include "Common/Timer.h"

#include "CommAddress.h"
#include "CommBuf.h"
#include "IOHandlerData.h"
#include "IOHandlerDatagram.h"
#include "ProxyMap.h"

namespace Hypertable {

  class IOHandlerAccept;

  class HandlerMap : public ReferenceCount {

  public:

  HandlerMap() : m_proxies_loaded(false) { }

    int32_t insert_handler(IOHandlerAccept *handler);

    int32_t insert_handler(IOHandlerData *handler);

    int32_t insert_handler(IOHandlerDatagram *handler);

    int checkout_handler(const CommAddress &addr, IOHandlerAccept **handler);

    int checkout_handler(const CommAddress &addr, IOHandlerData **handler);

    int checkout_handler(const CommAddress &addr, IOHandlerDatagram **handler);

    int contains_data_handler(const CommAddress &addr);

    void decrement_reference_count(IOHandler *handler);

    int set_alias(const InetAddr &addr, const InetAddr &alias);

    int remove_handler(IOHandler *handler);

    void decomission_handler(IOHandler *handler);

    void decomission_all();

    bool destroy_ok(IOHandler *handler);

    bool translate_proxy_address(const CommAddress &proxy_addr, CommAddress &addr);

    void purge_handler(IOHandler *handler);

    void wait_for_empty();

    int add_proxy(const String &proxy, const String &hostname, const InetAddr &addr);

    /**
     * Returns the proxy map
     *
     * @param proxy_map reference to proxy map to be filled in
     */
    void get_proxy_map(ProxyMapT &proxy_map);

    void update_proxy_map(const char *message, size_t message_len);

    bool wait_for_proxy_map(Timer &timer);

  private:

    int propagate_proxy_map(ProxyMapT &mappings);

    /**
     * Translates CommAddress into InetAddr (IP address)
     */
    int translate_address(const CommAddress &addr, InetAddr *inet_addr);

    int remove_handler_unlocked(IOHandler *handler);

    IOHandlerAccept *lookup_accept_handler(const InetAddr &addr);

    IOHandlerData *lookup_data_handler(const InetAddr &addr);

    IOHandlerDatagram *lookup_datagram_handler(const InetAddr &addr);

    Mutex                      m_mutex;
    boost::condition           m_cond;
    boost::condition           m_cond_proxy;
    SockAddrMap<IOHandlerAccept *>   m_accept_handler_map;
    SockAddrMap<IOHandlerData *>     m_data_handler_map;
    SockAddrMap<IOHandlerDatagram *> m_datagram_handler_map;
    std::set<IOHandler *>      m_decomissioned_handlers;
    ProxyMap                   m_proxy_map;
    bool                       m_proxies_loaded;
  };
  typedef boost::intrusive_ptr<HandlerMap> HandlerMapPtr;

}


#endif // HYPERTABLE_HANDLERMAP_H

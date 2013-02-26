/**
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
#include "Common/Compat.h"

#include "IOHandlerAccept.h"
#include "HandlerMap.h"
#include "ReactorFactory.h"

using namespace Hypertable;

int32_t HandlerMap::insert_handler(IOHandlerAccept *handler) {
  ScopedLock lock(m_mutex);
  HT_ASSERT(m_accept_handler_map.find(handler->get_address()) 
            == m_accept_handler_map.end());
  m_accept_handler_map[handler->get_local_address()] = handler;
  return Error::OK;
}

int32_t HandlerMap::insert_handler(IOHandlerData *handler) {
  ScopedLock lock(m_mutex);
  int error = Error::OK;
  HT_ASSERT(m_data_handler_map.find(handler->get_address())
            == m_data_handler_map.end());
  m_data_handler_map[handler->get_address()] = handler;
  if (ReactorFactory::proxy_master) {
    CommBufPtr comm_buf = m_proxy_map.create_update_message();
    comm_buf->write_header_and_reset();
    if ((error = handler->send_message(comm_buf)) != Error::OK)
      error = Error::COMM_BROKEN_CONNECTION;
  }
  return error;
}

int32_t HandlerMap::insert_handler(IOHandlerDatagram *handler) {
  ScopedLock lock(m_mutex);
  HT_ASSERT(m_datagram_handler_map.find(handler->get_local_address())
            == m_datagram_handler_map.end());
  m_datagram_handler_map[handler->get_local_address()] = handler;
  return Error::OK;
}


int HandlerMap::checkout_handler(const CommAddress &addr,
                                 IOHandlerAccept **handler) {
  ScopedLock lock(m_mutex);

  if ((*handler = lookup_accept_handler(addr.inet)) == 0)
    return Error::COMM_NOT_CONNECTED;

  HT_ASSERT(!(*handler)->is_decomissioned());

  (*handler)->increment_reference_count();

  return Error::OK;
}


int HandlerMap::checkout_handler(const CommAddress &addr,
                                 IOHandlerData **handler) {
  ScopedLock lock(m_mutex);
  InetAddr inet_addr;
  int error;

  if ((error = translate_address(addr, &inet_addr)) != Error::OK)
    return error;

  if ((*handler = lookup_data_handler(inet_addr)) == 0)
    return Error::COMM_NOT_CONNECTED;

  HT_ASSERT(!(*handler)->is_decomissioned());

  (*handler)->increment_reference_count();

  return Error::OK;
}

int HandlerMap::checkout_handler(const CommAddress &addr,
                                 IOHandlerDatagram **handler) {
  ScopedLock lock(m_mutex);

  if ((*handler = lookup_datagram_handler(addr.inet)) == 0)
    return Error::COMM_NOT_CONNECTED;

  HT_ASSERT(!(*handler)->is_decomissioned());

  (*handler)->increment_reference_count();

  return Error::OK;
}

void HandlerMap::decrement_reference_count(IOHandler *handler) {
  ScopedLock lock(m_mutex);
  handler->decrement_reference_count();
}

int HandlerMap::contains_data_handler(const CommAddress &addr) {
  ScopedLock lock(m_mutex);
  IOHandlerData *handler;
  InetAddr inet_addr;
  int error;

  if ((error = translate_address(addr, &inet_addr)) != Error::OK)
    return error;

  if ((handler = lookup_data_handler(inet_addr)) == 0)
    return Error::COMM_NOT_CONNECTED;

  return Error::OK;
}

int HandlerMap::set_alias(const InetAddr &addr, const InetAddr &alias) {
  ScopedLock lock(m_mutex);
  SockAddrMap<IOHandlerData *>::iterator iter;

  if (m_data_handler_map.find(alias) != m_data_handler_map.end())
    return Error::COMM_CONFLICTING_ADDRESS;

  if ((iter = m_data_handler_map.find(addr)) == m_data_handler_map.end())
    return Error::COMM_NOT_CONNECTED;

  (*iter).second->set_alias(alias);
  m_data_handler_map[alias] = (*iter).second;

  return Error::OK;
}

int HandlerMap::remove_handler_unlocked(IOHandler *handler) {
  SockAddrMap<IOHandlerAccept *>::iterator aiter;
  SockAddrMap<IOHandlerData *>::iterator diter;
  SockAddrMap<IOHandlerDatagram *>::iterator dgiter;
  InetAddr local_addr = handler->get_local_address();
  InetAddr remote_addr;
  int error;

  if ((error = translate_address(handler->get_address(), &remote_addr)) != Error::OK)
    return error;
  if ((diter = m_data_handler_map.find(remote_addr)) != m_data_handler_map.end()) {
    HT_ASSERT(handler == diter->second);
    m_data_handler_map.erase(diter);
    // Remove alias
    remote_addr = handler->get_alias();
    if ((diter = m_data_handler_map.find(remote_addr)) != m_data_handler_map.end()) {
      HT_ASSERT(handler == diter->second);
      m_data_handler_map.erase(diter);
    }
  }
  else if ((dgiter = m_datagram_handler_map.find(local_addr))
           != m_datagram_handler_map.end()) {
    HT_ASSERT(handler == dgiter->second);
    m_datagram_handler_map.erase(dgiter);
  }
  else if ((aiter = m_accept_handler_map.find(local_addr))
           != m_accept_handler_map.end()) {
    HT_ASSERT(handler == aiter->second);
    m_accept_handler_map.erase(aiter);
  }
  else
    return Error::COMM_NOT_CONNECTED;
  return Error::OK;
}

int HandlerMap::remove_handler(IOHandler *handler) {
  ScopedLock lock(m_mutex);
  return remove_handler_unlocked(handler);
}

void HandlerMap::decomission_handler(IOHandler *handler) {
  ScopedLock lock(m_mutex);
  if (remove_handler_unlocked(handler) != Error::OK) {
    HT_ASSERT(m_decomissioned_handlers.count(handler) > 0);
    return;
  }
  m_decomissioned_handlers.insert(handler);
  handler->decomission();
}

void HandlerMap::decomission_all() {
  ScopedLock lock(m_mutex);
  SockAddrMap<IOHandlerAccept *>::iterator aiter;
  SockAddrMap<IOHandlerData *>::iterator diter;
  SockAddrMap<IOHandlerDatagram *>::iterator dgiter;

  // IOHandlerData 
  for (diter = m_data_handler_map.begin(); diter != m_data_handler_map.end(); ++diter) {
    m_decomissioned_handlers.insert(diter->second);
    diter->second->decomission();
  }
  m_data_handler_map.clear();

  // IOHandlerDatagram
  for (dgiter = m_datagram_handler_map.begin();
       dgiter != m_datagram_handler_map.end(); ++dgiter) {
    m_decomissioned_handlers.insert(dgiter->second);
    dgiter->second->decomission();
  }
  m_datagram_handler_map.clear();

  // IOHandlerAccept
  for (aiter = m_accept_handler_map.begin();
       aiter != m_accept_handler_map.end(); ++aiter) {
    m_decomissioned_handlers.insert(aiter->second);
    aiter->second->decomission();
  }
  m_accept_handler_map.clear();
}

bool HandlerMap::destroy_ok(IOHandler *handler) {
  ScopedLock lock(m_mutex);
  bool is_decomissioned = m_decomissioned_handlers.count(handler) > 0;
  HT_ASSERT(!is_decomissioned || handler->is_decomissioned());
  return is_decomissioned && handler->reference_count() == 0;
}

bool HandlerMap::translate_proxy_address(const CommAddress &proxy_addr, CommAddress &addr) {
  InetAddr inet_addr;
  String hostname;
  HT_ASSERT(proxy_addr.is_proxy());
  if (!m_proxy_map.get_mapping(proxy_addr.proxy, hostname, inet_addr))
    return false;
  addr.set_inet(inet_addr);
  return true;
}

void HandlerMap::wait_for_empty() {
  ScopedLock lock(m_mutex);
  while (!m_decomissioned_handlers.empty())
    m_cond.wait(lock);
}

void HandlerMap::purge_handler(IOHandler *handler) {
  {
    ScopedLock lock(m_mutex);
    HT_ASSERT(m_decomissioned_handlers.count(handler) > 0);
    HT_ASSERT(handler->reference_count() == 0);
    m_decomissioned_handlers.erase(handler);
    if (m_decomissioned_handlers.empty())
      m_cond.notify_all();
  }
  handler->disconnect();
  delete handler;
}

int HandlerMap::add_proxy(const String &proxy, const String &hostname, const InetAddr &addr) {
  ScopedLock lock(m_mutex);
  ProxyMapT new_map, invalidated_map;

  m_proxy_map.update_mapping(proxy, hostname, addr, invalidated_map, new_map);

  foreach_ht(const ProxyMapT::value_type &v, invalidated_map) {
    IOHandler *handler = lookup_data_handler(v.second.addr);
    if (handler)
      handler->set_proxy("");
  }

  foreach_ht(const ProxyMapT::value_type &v, new_map) {
    IOHandler *handler = lookup_data_handler(v.second.addr);
    if (handler)
      handler->set_proxy(v.first);
  }

  return propagate_proxy_map(new_map);
}


void HandlerMap::get_proxy_map(ProxyMapT &proxy_map) {
  m_proxy_map.get_map(proxy_map);
}


void HandlerMap::update_proxy_map(const char *message, size_t message_len) {
  ScopedLock lock(m_mutex);
  String mappings(message, message_len);
  ProxyMapT new_map, invalidated_map;

  HT_ASSERT(!ReactorFactory::proxy_master);

  m_proxy_map.update_mappings(mappings, invalidated_map, new_map);

  foreach_ht(const ProxyMapT::value_type &v, invalidated_map) {
    IOHandler *handler = lookup_data_handler(v.second.addr);
    if (handler)
      handler->set_proxy("");
  }

  foreach_ht(const ProxyMapT::value_type &v, new_map) {
    IOHandler *handler = lookup_data_handler(v.second.addr);
    if (handler)
      handler->set_proxy(v.first);
  }

  m_proxies_loaded = true;
  m_cond_proxy.notify_all();
}

bool HandlerMap::wait_for_proxy_map(Timer &timer) {
  ScopedLock lock(m_mutex);
  boost::xtime drop_time;

  timer.start();

  while (!m_proxies_loaded) {
    boost::xtime_get(&drop_time, boost::TIME_UTC_);
    xtime_add_millis(drop_time, timer.remaining());
    if (!m_cond_proxy.timed_wait(lock, drop_time))
      return false;
  }
  return true;
}

int HandlerMap::propagate_proxy_map(ProxyMapT &mappings) {
  int last_error = Error::OK;

  if (mappings.empty())
    return Error::OK;

  SockAddrMap<IOHandlerData *>::iterator iter;
  String mapping;

  foreach_ht(const ProxyMapT::value_type &v, mappings)
    mapping += v.first + "\t" + v.second.hostname + "\t" + InetAddr::format(v.second.addr) + "\n";

  uint8_t *buffer = new uint8_t [ mapping.length() + 1 ];
  strcpy((char *)buffer, mapping.c_str());
  boost::shared_array<uint8_t> payload(buffer);
  CommHeader header;
  header.flags |= CommHeader::FLAGS_BIT_PROXY_MAP_UPDATE;
  for (iter = m_data_handler_map.begin(); iter != m_data_handler_map.end(); ++iter) {
    IOHandlerData *handler = iter->second;
    if (handler) {
      CommBufPtr comm_buf = new CommBuf(header, 0, payload, mapping.length()+1);
      comm_buf->write_header_and_reset();
      int error = handler->send_message(comm_buf);
      if (error != Error::OK) {
        HT_ERRORF("Unable to propagate proxy mappings to %s - %s",
                  InetAddr(handler->get_address()).format().c_str(),
                  Error::get_text(error));
        last_error = error;
      }
    }
  }
  return last_error;
}

    /**
     * Translates CommAddress into INET socket address
     */
int HandlerMap::translate_address(const CommAddress &addr, InetAddr *inet_addr) {
  String hostname;

  HT_ASSERT(addr.is_set());

  if (addr.is_proxy()) {
    if (!m_proxy_map.get_mapping(addr.proxy, hostname, *inet_addr))
      return Error::COMM_INVALID_PROXY;
  }
  else
    memcpy(inet_addr, &addr.inet, sizeof(InetAddr));

  return Error::OK;
}

IOHandlerAccept *HandlerMap::lookup_accept_handler(const InetAddr &addr) {
  SockAddrMap<IOHandlerAccept *>::iterator iter = m_accept_handler_map.find(addr);
  if (iter != m_accept_handler_map.end())
    return iter->second;
  return 0;
}

IOHandlerData *HandlerMap::lookup_data_handler(const InetAddr &addr) {
  SockAddrMap<IOHandlerData *>::iterator iter = m_data_handler_map.find(addr);
  if (iter != m_data_handler_map.end())
    return iter->second;
  return 0;
}

IOHandlerDatagram *HandlerMap::lookup_datagram_handler(const InetAddr &addr) {
  SockAddrMap<IOHandlerDatagram *>::iterator iter = m_datagram_handler_map.find(addr);
  if (iter != m_datagram_handler_map.end())
    return iter->second;
  return 0;
}

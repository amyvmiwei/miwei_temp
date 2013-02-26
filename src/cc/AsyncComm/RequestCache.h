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

/** @file
 * Declarations for RequestCache.
 * This file contains type declarations for RequestCache, a class
 * that is used to hold pending request data.
 */

#ifndef HYPERTABLE_REQUESTCACHE_H
#define HYPERTABLE_REQUESTCACHE_H

#include <boost/thread/xtime.hpp>

#include "Common/HashMap.h"

#include "DispatchHandler.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  class IOHandler;

  /** Class used to hold pending request callback handlers.  One RequestCache object is
   * associated with each Reactor.  When a request is sent (see Comm#send_request)
   * an entry, which includes the response handler, is inserted into the
   * RequestCache.  When the corresponding response is receive, the response
   * handler is obtained by looking up the corresponding request ID in this cache.
   */
  class RequestCache {

    /** Internal cache node structure.
     */
    struct CacheNode {
      struct CacheNode  *prev, *next;  //!< Doubly-linked list pointers
      boost::xtime       expire;  //!< Absolute expiration time
      uint32_t           id;      //!< Request ID
      IOHandler         *handler; //!< IOHandler associated with this request
      /// Callback handler to which MESSAGE, TIMEOUT, ERROR, and DISCONNECT
      /// events are delivered
      DispatchHandler   *dh;
    };

    /// RequestID-to-CacheNode map
    typedef hash_map<uint32_t, CacheNode *> IdHandlerMap;

  public:

    /// Constructor.
    RequestCache() : m_id_map(), m_head(0), m_tail(0) { return; }

    /** Inserts pending request callback handler into cache.
     * @param id Request ID
     * @param handler IOHandler associated with 
     * @param dh Callback handler to which MESSAGE, TIMEOUT, DISCONNECT events
     * are delivered
     * @param expire Absolute expiration time of request
     */
    void insert(uint32_t id, IOHandler *handler, DispatchHandler *dh,
                boost::xtime &expire);

    /** Removes a request from the cache.
     * @param id Request ID
     * @return Pointer to request's dispatch handler
     */
    DispatchHandler *remove(uint32_t id);

    /** Removes next request that has timed out.  This method finds the first
     * request starting from the head of the list and removes it and returns
     * it's associated handler information if it has timed out.  During the
     * search, it physically removes any cache nodes corresponding to requests
     * that have been purged.
     * @param now Current time
     * @param handlerp Return parameter to hold pointer to associated IOHandler
     *                 of timed out request
     * @param next_timeout Pointer to xtime variable to hold expiration time
     * of next request <b>after</b> timed out request, set to 0 if cache is empty
     * @return Pointer to timed out dispatch handler, or 0 if none
     */
    DispatchHandler *get_next_timeout(boost::xtime &now, IOHandler *&handlerp,
                                      boost::xtime *next_timeout);

    /** Purges all requests assocated with <code>handler</code>.  This
     * method walks the entire cache and purges all requests whose
     * handler is equal to <code>handler</code>.  For each purged
     * request, an ERROR event with error code <code>error</code> is
     * delivered via the request's dispatch handler.
     * @param handler IOHandler of requests to purge
     * @param error Error code to be delivered with ERROR event
     */
    void purge_requests(IOHandler *handler, int32_t error);

  private:
    IdHandlerMap  m_id_map; //!< RequestID-to-CacheNode map
    CacheNode    *m_head;   //!< Head of doubly-linked list
    CacheNode    *m_tail;   //!< Tail of doubly-linked list
  };
}

#endif // HYPERTABLE_REQUESTCACHE_H

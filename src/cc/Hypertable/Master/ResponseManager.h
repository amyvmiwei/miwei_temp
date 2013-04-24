/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
 * Declarations for ResponseManager.
 * This file contains declarations for ResponseManager, a class for managing
 * the sending operation results back to requesting clients.
 */

#ifndef HYPERTABLE_RESPONSEMANAGER_H
#define HYPERTABLE_RESPONSEMANAGER_H

#include <list>
#include <map>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/properties.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/thread/condition.hpp>

#include "AsyncComm/Comm.h"

#include "Common/Mutex.h"
#include "Common/Thread.h"
#include "Common/Time.h"

#include "Hypertable/Lib/MetaLogWriter.h"

#include "Context.h"
#include "Operation.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  using namespace boost::multi_index;

  /** Implementation shared by ResponseManager objects.
   * Implementation shared by multiple ResponseManager objets.
   * The reason this class was introduced is because the ResponseManger
   * is a "runnable" class and is passed to the Thread constructor by value, so
   * to ensure that the object gets copied without problems, this class was
   * introduced to encapsulate the implementation (see Pimpl idiom).  The
   * ResponseManger class contains just a private pointer to an object of this
   * type which allows the ResponseManager object to be copied by value while
   * retaining the same implementation.
   */
  class ResponseManagerContext {
  public:

    /** Constructor.
     * @param mmlw Smart pointer to MML writer
     */
    ResponseManagerContext(MetaLog::WriterPtr &mmlw) : mml_writer(mmlw), shutdown(false) {
      comm = Comm::instance();
    }
    
    class OperationRec {
    public:
      OperationRec(OperationPtr &_op) : op(_op) { }
      OperationPtr op;
      HiResTime expiration_time() const { return op->expiration_time(); }
      int64_t id() const { return op->id(); }
    };

    typedef boost::multi_index_container<
      OperationRec,
      indexed_by<
      sequenced<>,
      ordered_non_unique<const_mem_fun<OperationRec, HiResTime,
                                       &OperationRec::expiration_time> >,
      hashed_unique<const_mem_fun<OperationRec, int64_t,
                                  &OperationRec::id> >
    >
    > OperationList;

    typedef OperationList::nth_index<0>::type OperationSequence;
    typedef OperationList::nth_index<1>::type OperationExpirationTimeIndex;
    typedef OperationList::nth_index<2>::type OperationIdentifierIndex;

    class DeliveryRec {
    public:
      HiResTime expiration_time;
      int64_t id;
      EventPtr event;
    };

    typedef boost::multi_index_container<
      DeliveryRec,
      indexed_by<
      sequenced<>,
      ordered_non_unique<member<DeliveryRec, HiResTime,
                                &DeliveryRec::expiration_time> >,
      hashed_unique<member<DeliveryRec, int64_t,
                           &DeliveryRec::id> > 
    >
    > DeliveryList;

    typedef DeliveryList::nth_index<0>::type DeliverySequence;
    typedef DeliveryList::nth_index<1>::type DeliveryExpirationTimeIndex;
    typedef DeliveryList::nth_index<2>::type DeliveryIdentifierIndex;

    /// %Mutex for serializing concurrent access
    Mutex mutex;

    /// Condition variable used to wait for operations to expire
    boost::condition cond;

    /// Pointer to comm layer
    Comm *comm;

    /// MML writer
    MetaLog::WriterPtr mml_writer;

    /// Flag signalling shutdown in progress
    bool shutdown;

    /// List of completed operations
    OperationList expirable_ops;

    /// List of delivery information records
    DeliveryList delivery_list;

    /// Queue of completed operations to be removed from MML
    std::list<OperationPtr> removal_queue;
  };

  /** Manages the sending of operation results back to clients.
   * Clients create and carry out master operations in two steps:
   *
   *  1. %Operation request is sent, operation is created at master and added
   *     to the OperationProcessor, and the operation ID is sent back to the
   *     client
   *  2. Client issues a FETCH_RESULT request, with the ID returned in step 1,
   *     to obtain the result of the operation
   *
   * The reason for this two-step process is so that master failover can
   * be handled in a transparently by the client.
   */
  class ResponseManager {
  public:

    /** Constructor.
     * @param context Pointer to shared context object
     */
    ResponseManager(ResponseManagerContext *context) : m_context(context) { }

    /** Thread run method.
     * This method is called
     */
    void operator()();

    /** Queues a completed operation and/or delivers response.
     * This method first checks to see if client delivery information has been
     * added for <code>operation</code>.  If so, it sends a response message
     * back to the client, otherwise it adds <code>operation</code> to
     * <code>m_context->expirable_ops</code>, deferring the delivery of the
     * response message until the corresponding delivery information has been
     * added.
     * @param operation Completed operation ready for response delivery
     */
    void add_operation(OperationPtr &operation);

    /** Adds response delivery information and/or delivers response.
     * This method first decodes the payload of <code>event</code> for the
     * operation ID and then checks to see if that operation has been added
     * to the <code>m_context->expirable_ops</code> list.  If it has, then a
     * response is immediately sent back to the client and the operation is
     * removed from the list.  If it hasn't, then an entry is added to
     * <code>m_context->delivery_list</code> which holds the return address to
     * where the response should be sent.
     * @param event Event representing a %COMMAND_FETCH_RESULT request
     */
    void add_delivery_info(EventPtr &event);
    void shutdown();

  private:
    /// Pointer to shared context object
    ResponseManagerContext *m_context;
  };

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_RESPONSEMANAGER_H

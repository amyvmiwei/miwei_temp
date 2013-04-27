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

    /** Record holding completed operation information.
     */
    class OperationRec {
    public:

      /** Constructor.
       * @param _op Smart pointer to completed operation
       */
      OperationRec(OperationPtr &_op) : op(_op) { }

      /** Returns expiration time of completed operation
       * @return %Operation expiration time
       */
      HiResTime expiration_time() const { return op->expiration_time(); }

      /** Returns unique ID for completed operation.
       * @return Unique operation identifier
       */
      int64_t id() const { return op->id(); }

      /** Smart pointer to completed operation. */
      OperationPtr op;
    };

    /** Multi-index container for completed operations.
     * This container contains indexes on the operation IDs and expiration
     * times.
     */
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

    /** Record holding response delivery information for an operation.
     */
    class DeliveryRec {
    public:
      /** Delivery expiration time specified by client. */
      HiResTime expiration_time;
      /** Corresponding operation identifier */
      int64_t id;
      /** Event object containing return address for operation result */
      EventPtr event;
    };

    /** Multi-index container holding response delivery information for
     * operations.  This container contains indexes on the operation IDs and
     * expiration times.
     */
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
   * A single object of this class is created to handle sending operation
   * results back to clients.  It is designed to have a single worker thread to
   * handle removal of expired and completed operations.  The object created
   * from this class should be passed into the Thread constructor of a single
   * worker thread to handle the removal of expired and completed operations.
   * This thread should live throughout the lifetime of the master and should
   * only be joined during master shutdown.  Clients create and carry out master
   * operations in two steps:
   *
   *  1. %Operation request is sent, operation is created at master and added
   *     to the OperationProcessor, and the operation ID is sent back to the
   *     client
   *  2. The client issues a FETCH_RESULT request, with the ID returned in step
   *     1, to obtain the result of the operation
   *
   * The reason for this two-step process is so that master failover can
   * be handled transparently by the client.  For each operation, there are
   * two scenarios handled by this class, depending on whether or not the
   * operation completes before the FETCH_RESULT request is received:
   *
   *   1. If the operation completes before the FETCH_RESULT request is
   *      received, the operation is added to
   *      <code>m_context->expirable_ops</code>.  As soon as the FETCH_RESULT
   *      request is received, the operation results are sent back and the
   *      operation is removed.
   *   2. If the FETCH_RESULT request is received before the operation
   *      completes, the response delivery information is add to
   *      <code>m_context->m_context->delivery_list</code>.  As soon
   *      as the operation completes and is added with a call to add_operation(),
   *      the operation results are sent back, the delivery record is removed
   *      from <code>m_context->delivery_list</code>, and the operation is
   *      removed.
   */
  class ResponseManager {
  public:

    /** Constructor.
     * @param context Pointer to shared context object
     */
    ResponseManager(ResponseManagerContext *context) : m_context(context) { }

    /** Worker thread run method.
     * This method is called by the worker thread to handle the removal of
     * expired and completed operations.  It waits on
     * <code>m_context->cond</code> for the next operation or delivery
     * information record to time out, or for completed operations to get added
     * to <code>m_context->removal_queue</code> by either the add_operation()
     * or add_delivery_info() method.  If an operation times out, it is
     * removed from <code>m_context->expirable_ops</code> and is added to
     * <code>m_context->removal_queue</code>.  If a delivery information record
     * times out, it is removed from <code>m_context->delivery_list</code>.
     * At the end of the wait loop, the operations in
     * <code>m_context->mml_writer->record_removal</code> are removed from the
     * MML with a call to <code>m_context->mml_writer->record_removal</code>
     * and <code>m_context->removal_queue</code> is cleared.
     */
    void operator()();

    /** Queues a completed operation and/or delivers response.
     * This method first checks <code>m_context->delivery_list</code> to see if
     * client delivery information has been added for <code>operation</code>.
     * If so, it sends a response message back to the client, removes the
     * corresponding delivery information record from
     * <code>m_context->delivery_list</code>, adds <code>operation</code> to 
     * <code>m_context->removal_queue</code>, and then signals
     * <code>m_context->cond</code> to notify the worker thread that
     * the operation can be removed.  If client delivery information has
     * not yet been added for <code>operation</code>, it is added to
     * <code>m_context->expirable_ops</code>, deferring the delivery of the
     * response message until the corresponding delivery information has been
     * added with a call to add_delivery_info().
     * @param operation Completed operation ready for response delivery
     */
    void add_operation(OperationPtr &operation);

    /** Adds response delivery information and/or delivers response.
     * This method first decodes the payload of <code>event</code> for the
     * operation ID and then checks to see if that operation has been added
     * to <code>m_context->expirable_ops</code>.  If it has been added, then a
     * response is immediately sent back to the client, the operation is
     * removed from <code>m_context->expirable_ops</code> and added to
     * <code>m_context->removal_queue</code>, and then
     * <code>m_context->cond</code> is signalled to notify the worker thread
     * that the operation can be removed.  If the corresponding operation has
     * not yet been added to <code>m_context->expirable_ops</code>, an entry
     * containing <code>event</code> is added to
     * <code>m_context->delivery_list</code> so that at a later time, when
     * the corresponding operation is added via add_operation(), the operation
     * result can be sent back to the client at the delivery address contained
     * within <code>event</code>.
     * @param event Event representing a %COMMAND_FETCH_RESULT request
     */
    void add_delivery_info(EventPtr &event);

    /** Initiates shutdown sequence.
     * Sets <code>m_context->shutdown</code> to <i>true</i> and singals
     * <code>m_context->cond</code> to force the worker thread to exit.
     */
    void shutdown();

  private:
    /// Pointer to shared context object
    ResponseManagerContext *m_context;
  };

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_RESPONSEMANAGER_H

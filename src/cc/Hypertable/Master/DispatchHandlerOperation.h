/*
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

/** @file
 * Declarations for DispatchHandlerOperation.
 * This file contains declarations for DispatchHandlerOperation, a base class
 * for dispatch handlers used by operations that issue a request to a set of
 * range servers.
 */

#ifndef Hypertable_Master_DispatchHandlerOperation_h
#define Hypertable_Master_DispatchHandlerOperation_h

#include "Context.h"

#include <Hypertable/Lib/RangeServer/Client.h>

#include <AsyncComm/Comm.h>
#include <AsyncComm/DispatchHandler.h>

#include <Common/StringExt.h>

#include <condition_variable>
#include <mutex>
#include <set>

namespace Hypertable {

  using namespace Lib;

  /** @addtogroup Master
   *  @{
   */

  /** DispatchHandler class for managing async RangeServer requests.
   * This class is a DispatchHandler class that is used for managing
   * asynchronous requests to a set of range servers.
   */
  class DispatchHandlerOperation : public DispatchHandler {

  public:

    /** Holds request result information.
     */
    class Result {
    public:
      /** Constructor.
       * @param loc Proxy name of server associated with result
       */
      Result(const String &loc) : location(loc), error(Error::REQUEST_TIMEOUT) { }

      /** Less-than operator comparing on #location member
       * @param other Other result object to compare
       * @result -1, 0, or 1 depending of whether this object is less-than,
       * equal-to, or greater-than <code>other</code>
       */
      bool operator<(const Result &other) const {
        return location.compare(other.location) < 0;
      }
      String location;  //!< Proxy name of range server
      int error;        //!< %Result error code
      String msg;       //!< %Result error message
    };

    /** Constructor.
     * @param context %Master context
     */
    DispatchHandlerOperation(ContextPtr &context);

    /** Starts asynchronous request.
     * This method first initializes #m_error_count to 0, #m_locations to
     * <code>locations</code>, and m_outstanding to
     * <code>locations.size()</code>, and then kicks of a request for each
     * location with a call to start().
     * @param locations Set of locations for which to issue requests
     */
    void start(StringSet &locations);

    /** Method overridden in derived class for issuing requests.
     * @param location Proxy name of server to issue request.
     */
    virtual void start(const String &location) = 0;

    /** Post-request hook method.
     * This method can be overridden by derived classes to do post-processing
     * on each request result.
     * @param event Response event
     */
    virtual void result_callback(const EventPtr &event) { }

    /** Process response event.
     * This method inserts <code>event</code> int #m_events set, decrements
     * #m_outstanding, and then signals #m_cond if #m_outstanding has dropped to
     * zero
     * @param event Response event
     */
    virtual void handle(EventPtr &event);

    /** Waits for requests to complete.
     * This method waits on #m_cond until #m_outstanding drops to zero and then
     * calls process_events().
     * @return <i>true</i> if all requests completed without error, <i>false</i>
     * otherwise.
     */
    bool wait_for_completion();

    /** Processes #m_events set.
     * For each event in #m_events, this method creates a Result object, sets
     * the error code and error message extracted from the event payload, and
     * adds the result object to the #m_results set.  This method also makes a
     * call to result_callback() for each event.
     */
    void process_events();

    /** Returns the Result set.
     * This method copies #m_results to the <code>results</code> parameter.
     * @param results Reference to Result set to be populated.
     */
    void get_results(std::set<Result> &results);

  protected:

    /// %Master context
    ContextPtr m_context;

    /// %Range server client object
    RangeServer::Client m_rsclient;

  private:

    /** Strict Weak Ordering for comparing events by source address. */
    struct LtEventPtr {
      bool operator()(const EventPtr &e1, const EventPtr &e2) const {
        return e1->addr < e2->addr;
      }
    };

    /// %Mutex for serializing concurrent access
    std::mutex m_mutex;

    /// Condition variable used to wait for completion
    std::condition_variable m_cond;

    /// Set of events generated by range server responses
    std::set<EventPtr, LtEventPtr> m_events;

    /// Outstanding request count that have not completed
    int m_outstanding {};

    /// %Error count
    int m_error_count {};

    /// Set of servers participating in operation
    StringSet m_locations;

    /// Set of result objects
    std::set<Result> m_results;
  };

  /// Smart pointer to DispatchHandlerOperation
  typedef std::shared_ptr<DispatchHandlerOperation> DispatchHandlerOperationPtr;

  /** @}*/
}

#endif // Hypertable_Master_DispatchHandlerOperation_h

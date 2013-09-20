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
 * Declarations for OperationProcessor.
 * This file contains declarations for OperationProcessor, a class for executing
 * operations that have a dependency relationship in reverse topological
 * order.
 */

#ifndef HYPERTABLE_OPERATIONPROCESSOR_H
#define HYPERTABLE_OPERATIONPROCESSOR_H

#include <list>
#include <map>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/properties.hpp>
#include <boost/thread/condition.hpp>

#include "Common/HashMap.h"
#include "Common/Mutex.h"
#include "Common/Properties.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"
#include "Common/Thread.h"

#include "Context.h"
#include "Operation.h"
#include "ResponseManager.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Runs a set of operaions with dependency relationships.
   */
  class OperationProcessor : public ReferenceCount {
  public:
    OperationProcessor(ContextPtr &context, size_t thread_count);
    void add_operation(OperationPtr &operation);
    void add_operation(Operation *operation) {
      OperationPtr op(operation);
      add_operation(op);
    }
    void add_operations(std::vector<OperationPtr> &operations);
    OperationPtr remove_operation(int64_t hash_code);
    void shutdown();
    void join();
    void wait_for_empty();
    void wait_for_idle();
    bool timed_wait_for_idle(boost::xtime expire_time);
    void graphviz_output(String &output);
    size_t size();
    bool empty();
    void wake_up();
    void unblock(const String &name);
    void activate(const String &name);
    void state_description(String &output);

  private:

    void add_operation_internal(OperationPtr &operation);

    struct operation_t {
      typedef boost::vertex_property_tag kind;
    };

    struct execution_time_t {
      typedef boost::vertex_property_tag kind;
    };

    struct label_t {
      typedef boost::vertex_property_tag kind;
    };

    struct busy_t {
      typedef boost::vertex_property_tag kind;
    };

    struct permanent_t {
      typedef boost::edge_property_tag kind;
    };

    typedef boost::adjacency_list<
      boost::listS, boost::listS, boost::bidirectionalS,
      boost::property<boost::vertex_index_t, std::size_t,
      boost::property<operation_t, OperationPtr,
      boost::property<execution_time_t, int,
      boost::property<label_t, String,
      boost::property<busy_t, bool> > > > >,
      boost::property<permanent_t, bool> >
    OperationGraph;

    typedef boost::graph_traits<OperationGraph> GraphTraits;

    typedef GraphTraits::vertex_descriptor Vertex;
    typedef GraphTraits::edge_descriptor Edge;

    typedef std::set<Vertex> VertexSet;

    struct vertex_info {
      vertex_info(Vertex v, bool t) : vertex(v), taken(t) { }
      vertex_info(Vertex v) : vertex(v), taken(false) { }
      Vertex vertex;
      bool taken;
    };
    typedef std::list<vertex_info> ExecutionList;

    typedef std::multimap<const String, Vertex> DependencyIndex;

    void add_dependencies(Vertex v, OperationPtr &operation);
    void add_exclusivity(Vertex v, const String &name);
    void add_dependency(Vertex v, const String &name);
    void add_obstruction(Vertex v, const String &name);
    void purge_from_dependency_index(Vertex v);
    void purge_from_exclusivity_index(Vertex v);
    void purge_from_obstruction_index(Vertex v);
    void add_edge(Vertex v, Vertex u);
    void add_edge_permanent(Vertex v, Vertex u);

    /** Retires (remove) an operation.
     * @param v Vertex of operation
     * @param operation Reference to operation smart pointer
     * @note <code>m_context.mutex</code> must be locked when calling this
     * method
     */
    void retire_operation(Vertex v, OperationPtr &operation);

    /** Updates dependency relationship of an operation.
     * @note <code>m_context.mutex</code> must be locked when calling this
     * method
     */
    void update_operation(Vertex v, OperationPtr &operation);

    /** Recomputes operation execution order.
     * @note <code>m_context.mutex</code> must be locked when calling this
     * method
     */
    void recompute_order();

    /** Loads <code>m_context.current</code> list with operations to be
     * executed.
     * @note <code>m_context.mutex</code> must be locked when calling this
     * method
     */
    bool load_current();

    typedef std::set<OperationPtr> PerpetualSet;
    
    class OperationVertex {
    public:
      OperationVertex() : vertex(0) { }
      OperationVertex(OperationPtr &op, Vertex &v) :
        operation(op), vertex(v) { }
      OperationPtr operation;
      Vertex vertex;
    };

    class ThreadContext {
    public:
      ThreadContext(ContextPtr &mctx);
      ~ThreadContext();
      Mutex mutex;
      boost::condition cond;
      boost::condition idle_cond;
      OperationProcessor *op;
      ContextPtr &master_context;
      OperationGraph graph;
      VertexSet current_active;
      hash_map<int64_t, OperationVertex> operation_hash;
      size_t current_blocked;
      StringSet exclusive_ops;
      ExecutionList current;
      ExecutionList::iterator current_iter;
      ExecutionList execution_order;
      ExecutionList::iterator execution_order_iter;
      DependencyIndex exclusivity_index;
      DependencyIndex dependency_index;
      DependencyIndex obstruction_index;
      PerpetualSet perpetual_ops;
      size_t busy_count;
      bool need_order_recompute;
      bool shutdown;
      bool paused;
      VertexSet live;
      ResponseManager *response_manager;
      boost::property_map<OperationGraph, execution_time_t>::type exec_time;
      boost::property_map<OperationGraph, operation_t>::type ops;
      boost::property_map<OperationGraph, label_t>::type label;
      boost::property_map<OperationGraph, busy_t>::type busy;
      boost::property_map<OperationGraph, permanent_t>::type permanent;
    };

    struct not_permanent {
      not_permanent(ThreadContext &context) : m_context(context) { }
      bool operator()(const Edge &e) const {
        return !m_context.permanent[e];
      }
      ThreadContext &m_context;
    };

    struct ltvertexinfo {
      ltvertexinfo(ThreadContext &context) : m_context(context) { }
      inline bool operator() (const vertex_info &vi1, const vertex_info &vi2) {
        if (m_context.exec_time[vi1.vertex] == m_context.exec_time[vi2.vertex])
          return vi1.taken && !vi2.taken;
        return m_context.exec_time[vi1.vertex] < m_context.exec_time[vi2.vertex];
      }
      ThreadContext &m_context;
    };

    class Worker {
    public:
      Worker(ThreadContext &context) : m_context(context) { return; }
      void operator()();
    private:
      ThreadContext &m_context;
    };

    ThreadContext m_context;
    ThreadGroup m_threads;
  };

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONPROCESSOR_H

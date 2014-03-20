/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
 * Declarations for Operation.
 * This file contains declarations for Operation, an abstract base class that
 * that represents a master operation, and from which specific/concrete
 * operation classes are derived.
 */

#ifndef HYPERTABLE_OPERATION_H
#define HYPERTABLE_OPERATION_H

#include <ctime>
#include <set>

#include "AsyncComm/Event.h"

#include "Common/Mutex.h"
#include "Common/ScopeGuard.h"
#include "Common/Time.h"

#include "Hypertable/Lib/MetaLogEntity.h"

#include "Context.h"
#include "MetaLogEntityTypes.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** %Master operation states. */
  namespace OperationState {

    /** Enumeration for operation states */
    enum {
      INITIAL = 0,
      COMPLETE = 1,
      UNUSED = 2,
      STARTED = 3,
      ASSIGN_ID = 4,
      ASSIGN_LOCATION = 5,
      ASSIGN_METADATA_RANGES = 6,
      LOAD_RANGE = 7,
      LOAD_ROOT_METADATA_RANGE = 8,
      LOAD_SECOND_METADATA_RANGE = 9,
      WRITE_METADATA = 10,
      CREATE_RS_METRICS = 11,
      VALIDATE_SCHEMA = 12,
      SCAN_METADATA = 13,
      ISSUE_REQUESTS = 14,
      UPDATE_HYPERSPACE = 15,
      ACKNOWLEDGE = 16,
      FINALIZE = 17,
      CREATE_INDEX = 18,
      CREATE_QUALIFIER_INDEX = 19,
      PREPARE = 20,
      COMMIT = 21,
      PHANTOM_LOAD = 22,
      REPLAY_FRAGMENTS = 23
    };
    /** Converts operation state constant to human readable string.
     * @param state %Operation state constant
     * @return Human readable string representation for <code>state</code>.
     */
    const char *get_text(int state);
  }

  /** %Master dependency strings */
  namespace Dependency {
    extern const char *INIT;
    extern const char *SERVERS;
    extern const char *ROOT;
    extern const char *METADATA;
    extern const char *SYSTEM;
    extern const char *USER;
    extern const char *RECOVER_SERVER;
    extern const char *RECOVERY_BLOCKER;
    extern const char *RECOVERY;
  }

  /** %Namespace operation flags */
  namespace NamespaceFlag {
    /** Enumeration for namespace operation flags */
    enum {
      CREATE_INTERMEDIATE = 0x0001,
      IF_EXISTS           = 0x0002,
      IF_NOT_EXISTS       = 0x0004
    };
  }

  /** Set of dependency string */
  typedef std::set<String> DependencySet;

  /** Abstract base class for master operations.
   * The master is implemented as a dependency graph of operations that
   * are executed by the OperationProcessor.  Each operation is implemented as a
   * state machine and has a dependency relationship with other operations. This
   * class is the base class for all operations and defines a common interface.
   * The execute() method is called by the OperationProcessor to run the
   * operation's state machine and the dependency relationship is defined by
   * sets of dependency strings returned by the following methods:
   *
   *   - #exclusivities
   *   - #dependencies
   *   - #obstructions
   * 
   * See OperationProcessor for details on how the operation dependency graph
   * is setup and how operations are carried out.
   */
  class Operation : public MetaLog::Entity {
  public:

    /** Constructor with operation type specifier.
     * Initializes #m_state to OperationState::INITIAL, initializes
     * #m_expiration_time to number of milliseconds in the future as specified
     * by the <code>Hypertable.Request.Timeout</code> property, and initializes
     * #m_hash_code to the <i>id</i> field of MetaLog::Entity#header.
     * @param context %Master context
     * @param type %Operation type
     */
    Operation(ContextPtr &context, int32_t type);

    /** Constructor with request Event and operation type specifier.
     * Constructs an operation from a client request read off the wire.  Derived
     * class constructor should call decode_request() to decode request
     * parameters from <code>event</code>. Initializes #m_expiration_time to
     * number of milliseconds in the future as specified by the
     * <code>Hypertable.Request.Timeout</code> property and initializes
     * #m_hash_code to the <i>id</i> field of MetaLog::Entity#header.
     * @param context %Master context
     * @param event Event object
     * @param type %Operation type
     */
    Operation(ContextPtr &context, EventPtr &event, int32_t type);

    /** Constructor with MetaLog::EntityHeader.
     * Constructs an operation from <code>header_</code> read from a MetaLog.
     * After object has been constructed, the MetaLogReader class will
     * read the rest of the MetaLog entry and will reconstruct the object
     * state with a call to decode().  This constructor initializes #m_hash_code
     * to the <i>id</i> field of MetaLog::Entity#header.
     * @note Object initialization wont be complete until after the call to
     * decode(), so any post-initialization setup should be performed at the
     * end of the decode() method.
     * @param context %Master context
     * @param header_ MetaLog header
     */
    Operation(ContextPtr &context, const MetaLog::EntityHeader &header_);

    /** Destructor */
    virtual ~Operation() { }

    /** Executes (carries out) the operation.
     * This method is called by the OperationProcessor to carry out the
     * operation.  After calling this method, the OperationProcessor will check
     * the state of the operation with a call to get_state().  If the state is
     * OperationState::COMPLETE, then it assumes that the operation is complete
     * and will destory it.  Otherwise, it will remain in the operation dependency
     * graph and will get re-executed at a later time.  After the call to this
     * method, the OperationProcessor will re-compute the operation dependency
     * graph (which may have changed due to the removal of this operation or if
     * there were modifications to #m_exclusivities, #m_dependencies, or
     * #m_obstructions) and will continue operation execution in the approprate
     * order.
     */
    virtual void execute() = 0;

    /** Name of operation used for exclusivity.
     * An operation can be marked <i>exclusive</i> (see exclusvive()) which tells
     * the Operation processor that only one operation of this name may be added
     * to the dependency graph.  If an attempt to add an an exclusive operation
     * is made and the OperationProcessor already contains an exclusive operation
     * with the same name, the attempt will fail and will result in
     * an Exception with error code Error::MASTER_OPERATION_IN_PROGRESS.
     * @return Name of operation used to enforce exclusivity constraint
     */
    virtual const String name() = 0;

    /** Human readable label for operation.
     * This method is used to generate a human readable string describing the
     * operation and is typically used for generating log messages.
     * @return Human readable string describing operation
     */
    virtual const String label() = 0;

    /** Human readable operation label used in graphviz output.
     * The OperationProcessor periodically generates graphviz output
     * describing the operation dependency graph.  This method is
     * simlar to label(), but can be modified to produce a string that
     * renders better in the dependency graph visualization.  It is typically
     * the same as what's produce by label(), but may contain newlines or
     * elided strings to reduce the width of the label.
     * @return Human readable operation label for use with graphviz.
     */
    virtual const String graphviz_label() { return label(); }

    /** Indicates if operation is exclusive.
     * An operation can be designated as <i>exclusive</i> which means that only
     * one operation of this type may be added to the OperationProcessor at
     * any given time.  This method is used in conjunction with the name()
     * method to determine if the operation can be added to the
     * OperationProcessor.  If this method returns <i>true</i> and another
     * exclusive operation exists in the OperationProcessor with the same name
     * as returned by name(), then the attempt to add the operation will throw
     * an Exception with error code Error::MASTER_OPERATION_IN_PROGRESS.
     * @return <i>true</i> if operation is exclusive, <i>false</i> otherwise.
     */
    virtual bool exclusive() { return false; }

    /** Decodes initial operation state from Event payload.
     * Some operations can be created in response to request events sent to the
     * master by clients.  For these types of operations, this method should be
     * overridden to decode the initial operation state from the Event object
     * created in response to a client request.  This method should be called
     * with <code>bufp</code> initialized to Event::payload and
     * <code>remainp</code> initialized from Event::payload_len.  The payload
     * contains initial operation state (request parameters) to be used to
     * initialize the operation.
     * @param bufp Address of buffer pointer (initialized to Event payload)
     * @param remainp Address of integer indicating how much buffer remains
     */
    virtual void decode_request(const uint8_t **bufp, size_t *remainp) { }

    /** Encoded length of operation state.
     * @return Length of encoded operation state.
     */
    virtual size_t encoded_state_length() const = 0;

    /** Encode operation state.
     * This method is called by encode() to encode state that is specific
     * to the operation.  The encoded state is written to the memory location
     * pointed to by <code>*bufp</code>, which is modified to point to the
     * first byte after the encoded state.
     * @param bufp Address of pointer to destination buffer (modified by call)
     */
    virtual void encode_state(uint8_t **bufp) const = 0;

    /** Decode operation state.
     * This method is called by decode() to decode state that is specific
     * to the operation.  The encoded state should start at the memory location
     * pointed to by <code>*bufp</code>, and if successfully decoded, will be
     * modified to point to the first byte past the encoded state.  The
     * <code>remainp</code> parameter is a pointer to an integer holding the
     * number of valid/readable bytes pointed to by <code>*bufp</code> and
     * if decoding is sucessful, will be decremented by the length of the
     * encoded state.
     * @param bufp Address of pointer to destination buffer
     * @param remainp Address of integer holding amount of remaining buffer
     */
    virtual void decode_state(const uint8_t **bufp, size_t *remainp) = 0;

    /** Write human readable operation state to output stream.
     * This method is called by display() to write a human readable string
     * representation of the operation state to <code>os</code>
     * @param os Output stream to which state string is to be written
     */
    virtual void display_state(std::ostream &os) = 0;

    /** Returns version of encoding format.
     * This is method returns the version of the encoding format.
     * @return Version of encoding format.
     */
    virtual uint16_t encoding_version() const = 0;

    /** Length of encoded operation result.
     * This method returns the length of the encoded result, which is
     * encoded as follows:
     * <pre>
     *   i32  error code
     *   vstr error message (if error code != Error::OK)
     * </pre>
     * @return length of encoded result
     */
    virtual size_t encoded_result_length() const;

    /** Encode operation result.
     * Encodes the result of the operation as follows:
     * <pre>
     *   i32  error code
     *   vstr error message (if error code != Error::OK)
     * </pre>
     * This method is called by encode() to handle encoding of the operation
     * result.
     * @param bufp Address of pointer to destination buffer
     */
    virtual void encode_result(uint8_t **bufp) const;

    /** Decode operation result.
     * Decodes the result of the operation encoded as follows:
     * <pre>
     *   i32  error code
     *   vstr error message (if error code != Error::OK)
     * </pre>
     * This method is called by decode() to handle decoding of the operation
     * result.
     * @param bufp Address of pointer to encoded result
     * @param remainp Address of integer holding amount of remaining buffer
     */
    virtual void decode_result(const uint8_t **bufp, size_t *remainp);

    /** Length of encoded operation.
     * Length of encoded operation.  See encode() for description of encoding
     * format.
     * @return length of encoded operation
     */
    virtual size_t encoded_length() const;

    /** Encode operation.
     * Encodes operation in the following format:
     * <pre>
     *   i32  operation state
     *   i64  expiration time seconds
     *   i32  expiration time nanoseconds
     *   i32  remove approvals flag
     *   if (m_state == %COMPLETE)
     *     i64  hash code
     *     result
     *   else
     *     state
     *     exclusivities
     *     dependencies
     *     obstructions
     *   end
     * </pre>
     */
    virtual void encode(uint8_t **bufp) const;

    /** Decode operation.
     * Decodes operation.  See encode() for description of encoding format.
     * Upon successful decode, this method will modify <code>*bufp</code>
     * to point to the first byte past the encoded result and will decrement
     * <code>*remainp</code> by the length of the encoded result.
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by
     * <code>*bufp</code> (decremented by call).
     * @param definition_version Version of DefinitionMaster
     */
    virtual void decode(const uint8_t **bufp, size_t *remainp,
                        uint16_t definition_version);

    /** Write human readable string represenation of operation to output stream.
     * @param os Output stream to which string is written
     */
    virtual void display(std::ostream &os);

    /** Operation identifier.
     * Returns an integer identifier that uniquely identifies this operation.
     * The ID that is returned is the same as the <i>id</i> field of the
     * Metalog::Entity#header member of the base class.
     * @return operation identifier.
     */
    int64_t id() { return header.id; }

    /** Returns operation expiration time.
     * Operations have an expiration time held in the #m_expiration_time member.
     * It is initialized to either the value of the property
     * <code>Hypertable.Request.Timeout</code> or the timeout value of the
     * client request that caused the operation to be created.  Currently
     * it is only used by the ResponseManager class.  When a completed
     * operation is added to the ResponseManager, it will be held there until
     * either a FETCH_RESULT command for the operation has be received from
     * the client, or the expiration time has been reached, after which the
     * operation will be permanently removed.
     * @return Expiration time of the operation
     */
    HiResTime expiration_time() { ScopedLock lock(m_mutex); return m_expiration_time; }

    /// Sets the remove approvals bit mask.
    /// @param mask Bitmask to use as remove approvals mask.
    /// @see remove_approval_add, get_remove_approval_mask
    void set_remove_approval_mask(uint16_t mask) {
      ScopedLock lock(m_mutex);
      m_remove_approval_mask = mask;
    }

    /// Gets the remove approvals bit mask
    /// @return The remove approvals bit mask
    /// @see remove_approval_add, set_remove_approval_mask
    uint16_t get_remove_approval_mask() {
      ScopedLock lock(m_mutex);
      return m_remove_approval_mask;
    }

    /** Sets remove approval bits.
     * This method is used for operations that are to be removed explicitly.
     * It sets bits in #m_remove_approvals by bitwise OR'ing it with
     * <code>approval</code>.  Once the bits in #m_remove_approvals are set such
     * that #m_remove_approvals is equal to those returned by
     * #m_remove_approval_mask, the operation can be safely removed.
     * @see get_remove_approval_mask, set_remove_approval_mask, remove_if_ready
     * @param approval Integer flag indicating bits to be set in #m_remove_approvals
     */
    void remove_approval_add(uint16_t approval) {
      ScopedLock lock(m_mutex);
      m_remove_approvals |= approval;
    }

    /** Remove operation from MML if received all approvals.
     * This method is used for operations that are to be removed explicitly. It
     * removes the operation from the MML if #m_remove_approvals equals what is
     * returned by remove_approval_mask(), indicating that the operation can be
     * safely removed.
     * @see remove_explicitly, remove_approval_mask, remove_approval_add
     * @return <i>true</i> if operation was removed, <i>false</i> otherwise.
     */
    bool remove_if_ready();

    void complete_error(int error, const String &msg);
    void complete_error(Exception &e);
    void complete_ok(MetaLog::Entity *additional=0);

    virtual int64_t hash_code() const { return m_hash_code; }

    virtual void exclusivities(DependencySet &exclusivities);
    virtual void dependencies(DependencySet &dependencies);
    virtual void obstructions(DependencySet &obstructions);

    void add_exclusivity(const String &exclusivity) { m_exclusivities.insert(exclusivity); }
    void add_dependency(const String &dependency) { m_dependencies.insert(dependency); }
    void add_obstruction(const String &obstruction) { m_obstructions.insert(obstruction); }

    void swap_sub_operations(std::vector<Operation *> &sub_ops);

    void pre_run();
    void post_run();
    int32_t get_state() { ScopedLock lock(m_mutex); return m_state; }
    void set_state(int32_t state) { ScopedLock lock(m_mutex); m_state = state; }
    virtual bool is_perpetual() { return false; }
    bool block();
    bool unblock();
    bool is_blocked() { ScopedLock lock(m_mutex); return m_blocked; }
    bool is_complete() { ScopedLock lock(m_mutex); return m_state == OperationState::COMPLETE; }

    int32_t get_original_type() { return m_original_type; }
    void set_original_type(int32_t original_type) { m_original_type = original_type; }

    /** Sets the #m_ephemeral flag to <i>true</i>. */
    void set_ephemeral() {
      ScopedLock lock(m_mutex);
      m_ephemeral = true;
    }

  protected:
    ContextPtr m_context;
    EventPtr m_event;
    int32_t m_state {OperationState::INITIAL};
    int32_t m_error {};
    uint16_t m_remove_approvals {};
    uint16_t m_remove_approval_mask {};
    int32_t m_original_type {};
    String m_error_msg;

    /// Version of serialized operation
    uint16_t m_decode_version {};

    bool m_unblock_on_exit {};
    bool m_blocked {};

    /// Indicates if operation is ephemeral and does not get persisted to MML
    bool m_ephemeral {};

    // Expiration time (used by ResponseManager)
    HiResTime m_expiration_time;
    int64_t m_hash_code;
    DependencySet m_exclusivities;
    DependencySet m_dependencies;
    DependencySet m_obstructions;
    std::vector<Operation *> m_sub_ops;
  };

  /** Smart pointer to Operation */
  typedef intrusive_ptr<Operation> OperationPtr;

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_OPERATION_H

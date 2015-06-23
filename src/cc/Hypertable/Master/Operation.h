/* -*- c++ -*-
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

/// @file
/// Declarations for Operation.
/// This file contains declarations for Operation, an abstract base class that
/// that represents a master operation, and from which specific/concrete
/// operation classes are derived.

#ifndef Hypertable_Master_Operation_h
#define Hypertable_Master_Operation_h

#include "Context.h"
#include "MetaLogEntityTypes.h"

#include <Hypertable/Lib/MetaLogEntity.h>

#include <AsyncComm/Clock.h>
#include <AsyncComm/Event.h>

#include <Common/ScopeGuard.h>

#include <ctime>
#include <set>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// %Master operation states.
  namespace OperationState {

    /// Enumeration for operation states.
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
      REPLAY_FRAGMENTS = 23,
      CREATE_INDICES = 24,
      DROP_INDICES = 25,
      SUSPEND_TABLE_MAINTENANCE = 26,
      RESUME_TABLE_MAINTENANCE = 27,
      DROP_VALUE_INDEX = 28,
      DROP_QUALIFIER_INDEX = 29,
      RENAME_VALUE_INDEX = 30,
      RENAME_QUALIFIER_INDEX = 31
    };
    /// Converts operation state constant to human readable string.
    /// @param state %Operation state constant
    /// @return Human readable string representation for <code>state</code>.
    const char *get_text(int state);
  }

  /// %Master dependency strings
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

  /// Set of dependency string
  typedef std::set<String> DependencySet;

  /// Abstract base class for master operations.
  /// The master is implemented as a dependency graph of operations that
  /// are executed by the OperationProcessor.  Each operation is implemented as a
  /// state machine and has a dependency relationship with other operations. This
  /// class is the base class for all operations and defines a common interface.
  /// The execute() method is called by the OperationProcessor to run the
  /// operation's state machine and the dependency relationship is defined by
  /// sets of dependency strings returned by the following methods:
  ///
  ///   - #exclusivities
  ///   - #dependencies
  ///   - #obstructions
  /// 
  /// See OperationProcessor for details on how the operation dependency graph
  /// is setup and how operations are carried out.
  class Operation : public MetaLog::Entity {
  public:

    /** Constructor with operation type specifier.
     * Initializes state to OperationState::INITIAL, initializes
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

    /// Length of encoded operation result.
    /// This method returns the length of the encoded result
    /// @return length of encoded result
    /// @see encode_result() for encoding format.
    virtual size_t encoded_result_length() const;

    /// Encode operation result.
    /// This method is called by encode() to encode the result of the operation
    /// when it is in the OperationState::COMPLETE state.  The result is encoded
    /// in the following format:
    /// <table>
    ///   <tr>
    ///   <th>Encoding</th><th>Description</th>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>Error code</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>Error message (if error code != Error::OK)</td>
    ///   </tr>
    /// </table>
    /// @param bufp Address of pointer to destination buffer
    virtual void encode_result(uint8_t **bufp) const;

    /// Decode operation result.
    /// This method is called by decode() to handle decoding of the operation
    /// result.
    /// @param bufp Address of pointer to encoded result
    /// @param remainp Address of integer holding amount of remaining buffer
    /// @see encode_result() for encoding format.
    virtual void decode_result(const uint8_t **bufp, size_t *remainp);

    /** Returns version of encoding format of state.
     * This is method returns the version of the encoding format of the state.
     * @return Version of encoding format of state.
     */
    virtual uint8_t encoding_version_state() const = 0;

    /** Encoded length of operation state.
     * @return Length of encoded operation state.
     */
    virtual size_t encoded_length_state() const = 0;

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
     * @param version Encoding version
     * @param bufp Address of pointer to destination buffer
     * @param remainp Address of integer holding amount of remaining buffer
     */
    virtual void decode_state(uint8_t version, const uint8_t **bufp,
                              size_t *remainp) = 0;

    virtual void decode_state_old(uint8_t version, const uint8_t **bufp,
                                  size_t *remainp) = 0;

    /** Write human readable operation state to output stream.
     * This method is called by display() to write a human readable string
     * representation of the operation state to <code>os</code>
     * @param os Output stream to which state string is to be written
     */
    virtual void display_state(std::ostream &os) = 0;

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
    ClockT::time_point expiration_time() { std::lock_guard<std::mutex> lock(m_mutex); return m_expiration_time; }

    /// Sets the remove approvals bit mask.
    /// @param mask Bitmask to use as remove approvals mask.
    /// @see remove_approval_add, get_remove_approval_mask
    void set_remove_approval_mask(uint16_t mask) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_remove_approval_mask = mask;
    }

    /// Gets the remove approvals bit mask
    /// @return The remove approvals bit mask
    /// @see remove_approval_add, set_remove_approval_mask
    uint16_t get_remove_approval_mask() {
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_remove_approval_mask;
    }

    /** Sets remove approval bits.
     * This method is used for operations that are to be removed explicitly.
     * It sets bits in #m_remove_approvals by bitwise OR'ing it with
     * <code>approval</code>.  Once the bits in #m_remove_approvals are set such
     * that #m_remove_approvals is equal to those returned by
     * #m_remove_approval_mask, the operation can be safely removed.
     * @see get_remove_approval_mask, set_remove_approval_mask
     * @param approval Integer flag indicating bits to be set in #m_remove_approvals
     */
    bool remove_approval_add(uint16_t approval) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_remove_approvals |= approval;
      return m_remove_approvals == m_remove_approval_mask;
    }

    /// Checks if all remove approvals have been received.
    /// This member function will return <i>true</i> if the remove approval mask
    /// is non-zero and the approvals received (#m_remove_approvals) equals the
    /// mask.
    /// @return <i>true</i> if remove approvals required for removal have been
    /// received, <i>false</i> otherwise.
    bool removal_approved();

    /// Records operation state and additional entities to the MML.
    /// This member function builds up a vector of entities to record in the MML
    /// by adding the following:
    ///   - This operation.
    ///   - The <code>additional</code> entities.
    ///   - This operation's sub operations.
    ///
    /// For each entity to be recorded in the MML, if the entity is an operation
    /// and removal_approved() returns <i>true</i>, then the operation is marked
    /// for removal with a call to mark_for_removal().  The vector of entities
    /// is recorded to the MML.  Then, the operations that were marked for
    /// removal are removed from the reference manager.  Finally, the #m_sub_ops
    /// array is modified to hold the sub operation IDs that were not removed by
    /// this function.
    /// @param additional Additional entities to be recorded in MML
    void record_state(std::vector<MetaLog::EntityPtr> &additional);

    /// Records operation state to the MML.
    /// This member function creates an empty entity vector and passes it into a
    /// chained call to record_state().
    void record_state() {
      std::vector<MetaLog::EntityPtr> additional;
      record_state(additional);
    }

    /// Completes operation with error.
    /// <a name="complete_error1"></a>
    /// This member function does the following:
    ///   - Sets state to OperationState::COMPLETE
    ///   - Sets #m_error to <code>error</code>
    ///   - Sets #m_error_msg to <code>msg</code>
    ///   - Clears dependency, obstruction, and exclusivity sets
    ///   - Each sub operation has all of its remove approvals set.
    ///   - Persists state along with <code>additional</code> with a call to
    /// record_state()
    /// @param error %Error code of operation result
    /// @param msg %Error message of operation result
    /// @param additional Vector of additional entities to persist to MML
    void complete_error(int error, const String &msg, std::vector<MetaLog::EntityPtr> &additional);

    /// Completes operation with error.
    /// <a name="complete_error2"></a>
    /// This member function wraps <code>additional</code> into a vector and
    /// chains the call to <a href="#complete_error1">complete_error</a>
    /// @param error %Error code of operation result
    /// @param msg %Error message of operation result
    /// @param additional Additional entity to persist to MML
    void complete_error(int error, const String &msg, MetaLog::EntityPtr additional=MetaLog::EntityPtr());

    /// Completes operation with exception.
    /// This method chains the call to
    /// <a href="#complete_error1">complete_error</a> with
    /// <code>e.code()</code> and <code>e.what()</code> as the error code and
    /// message, respectively.
    /// @param e %Exception precipitating the operation completion
    /// @param additional Vector of additional entities to persist to MML
    void complete_error(Exception &e, std::vector<MetaLog::EntityPtr> &additional) {
      complete_error(e.code(), e.what(), additional);
    }

    /// Completes operation with exception.
    /// This method chains the call to
    /// <a href="#complete_error2">complete_error</a> with
    /// <code>e.code()</code> and <code>e.what()</code> as the error code and
    /// message, respectively.
    /// @param e %Exception precipitating the operation completion
    /// @param additional Vector of additional entity to persist to MML
    void complete_error(Exception &e, MetaLog::EntityPtr additional=MetaLog::EntityPtr()) {
      complete_error(e.code(), e.what(), additional);
    }

    void complete_ok(std::vector<MetaLog::EntityPtr> &additional);
    void complete_ok(MetaLog::EntityPtr additional=MetaLog::EntityPtr());

    virtual int64_t hash_code() const { return m_hash_code; }

    virtual void exclusivities(DependencySet &exclusivities);
    virtual void dependencies(DependencySet &dependencies);
    virtual void obstructions(DependencySet &obstructions);

    void add_exclusivity(const String &exclusivity) { m_exclusivities.insert(exclusivity); }
    void add_dependency(const String &dependency) { m_dependencies.insert(dependency); }
    void add_obstruction(const String &obstruction) { m_obstructions.insert(obstruction); }
    void add_obstruction_permanent(const String &obstruction) {
      m_obstructions_permanent.insert(obstruction);
    }

    void fetch_sub_operations(std::vector<std::shared_ptr<Operation> > &sub_ops);

    void pre_run();
    void post_run();
    int32_t get_state() { std::lock_guard<std::mutex> lock(m_mutex); return m_state; }
    void set_state(int32_t state) { std::lock_guard<std::mutex> lock(m_mutex); m_state = state; }
    virtual bool is_perpetual() { return false; }
    bool block();
    bool unblock();
    bool is_blocked() { std::lock_guard<std::mutex> lock(m_mutex); return m_blocked; }
    bool is_complete() { std::lock_guard<std::mutex> lock(m_mutex); return m_state == OperationState::COMPLETE; }

    int32_t get_original_type() { return m_original_type; }
    void set_original_type(int32_t original_type) { m_original_type = original_type; }

    /// Sets the <i>ephemeral</i> flag to <i>true</i>.
    void set_ephemeral() {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_ephemeral = true;
    }

    /// Gets the <i>ephemeral</i> flag
    bool ephemeral() {
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_ephemeral;
    }

    /// Get error code
    /// @return Error code
    int32_t get_error() {
      std::lock_guard<std::mutex> lock(m_mutex);
      HT_ASSERT(m_state == OperationState::COMPLETE);
      return m_error;
    }

    /// Get error message
    /// @return Error message
    String get_error_msg() {
      std::lock_guard<std::mutex> lock(m_mutex);
      HT_ASSERT(m_state == OperationState::COMPLETE);
      return m_error_msg;
    }

  protected:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    void decode_old(const uint8_t **bufp, size_t *remainp,
                    uint16_t definition_version);

    /// Handles the results of sub operations.
    /// For each sub operation, fetches the operation from the reference manager.
    /// Checks to see if sub operation resulted in an error, if so, calls
    /// complete_error() with the sub operation's error information and
    /// returns <i>false</i>.  Otherwise, the sub operation's remove approvals
    /// are set and the sub operation dependency string (see stage_subop()) is removed from
    /// #m_dependencies.  All of the sub operations are added to a local
    /// <i>additional</i> vector, then #m_sub_ops is cleared, and the
    /// <i>additional</i> vector is passed into a call to record_state().
    /// @return <i>true</i> if no sub operation is outstanding or if all of the
    /// sub operations completed without error, <i>false</i> otherwise.
    bool validate_subops();

    /// Stages a sub operation for execution.
    /// This member function creates the following dependency string:
    /// <pre>
    ///   this->name() + " subop " + operation->name() + operation->hash_code()
    /// </pre>
    /// and adds it as a dependency to the current operation (this) and also
    /// adds it as a permanent obstruction to <code>operation</code>, making the
    /// current operation dependent on the sub operation.  A remove approval
    /// mask of <code>0x01</code> is set for <code>operation</code>, and then
    /// <code>operation</code> is added to the reference manager and it's ID is
    /// pushed onto the end of #m_sub_ops.
    /// @param operation Sub operation to stage
    void stage_subop(std::shared_ptr<Operation> operation);

    /// Pointer to %Master context
    ContextPtr m_context;

    /// Pointer to client event (if any) that originated the operation
    EventPtr m_event;

    /// %Operation state
    int32_t m_state {OperationState::INITIAL};

    /// Remove approvals received
    uint16_t m_remove_approvals {};

    /// Remove approval mask
    uint16_t m_remove_approval_mask {};

    /// Original entity type read from MML (prior to conversion)
    int32_t m_original_type {};

    /// Result error code
    int32_t m_error {};

    /// Result error message
    String m_error_msg;

    /// Flag to signal operation to be unblocked on exit (post_run())
    bool m_unblock_on_exit {};

    /// Flag indicating if operation is blocked
    bool m_blocked {};

    /// Indicates if operation is ephemeral and does not get persisted to MML
    bool m_ephemeral {};

    /// Expiration time (used by ResponseManager)
    ClockT::time_point m_expiration_time;

    /// Hash code uniqely identifying operation
    int64_t m_hash_code;

    /// Set of exclusivities
    DependencySet m_exclusivities;

    /// Set of dependencies
    DependencySet m_dependencies;

    /// Set of obstructions
    DependencySet m_obstructions;

    /// Set of permanent obstructions
    DependencySet m_obstructions_permanent;

    /// Vector of sub operations IDs
    std::vector<int64_t> m_sub_ops;

  };

  /// Smart pointer to Operation
  typedef std::shared_ptr<Operation> OperationPtr;

  /// @}

} // namespace Hypertable

#endif // Hypertable_Master_Operation_h

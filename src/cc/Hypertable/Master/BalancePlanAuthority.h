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
 * Declarations for BalancePlanAuthority.
 * This file contains declarations for BalancePlanAuthority, a class that acts
 * as the central authority for all active balance plans.
 */

#ifndef HYPERTABLE_BALANCEPLANAUTHORITY_H
#define HYPERTABLE_BALANCEPLANAUTHORITY_H

#include <map>

#include <boost/thread/condition.hpp>

#include <Hypertable/Lib/BalancePlan.h>
#include <Hypertable/Lib/MetaLogWriter.h>
#include <Hypertable/Lib/RangeServerRecovery/Plan.h>

#include "Context.h"

namespace Hypertable {

  using namespace Lib;

  /// @addtogroup Master
  /// @{

  /** Central authority for balance plans.
   * This class maintains all currently active <i>balance plans</i>.  A balance
   * plan is a specification for the movement of a set of ranges from their
   * current location to a new location.  Balance plans are created in the
   * following circumstances:
   *
   *   - <b>Split</b> - When a range splits, a new range is created and is moved
   *     to a new range server.  A plan is created for the range, indicating
   *     where it is to be moved to.
   *   - <b>Load balancing</b> - A balance plan is created by the load balancer
   *     to record the source and destination locations of all moves in the
   *     balance operation.
   *   - <b>Failover</b> - When a range server fails, all of its ranges are
   *     moved to new locations.  A balance plan is created in this situation to
   *     record where the ranges are to be moved.
   *
   * There is one object of this class maintained by the Master and it is
   * persisted to the MML each time its state changes.
   */
  class BalancePlanAuthority : public MetaLog::Entity {
  public:

    /** Constructor.
     * This constructor constructs an empty object.  Persists the object state
     * in the MML.
     * @param context Master context
     * @param mml_writer MML writer for persisting state changes
     */
    BalancePlanAuthority(ContextPtr context, MetaLog::WriterPtr &mml_writer);

    /** Constructor with MetaLog header.
     * This constructor constructs an empty object to be populated by an MML
     * entity.
     * @param context Master context
     * @param mml_writer MML writer for persisting state changes
     * @param header_ MML entity header
     */
    BalancePlanAuthority(ContextPtr context, MetaLog::WriterPtr &mml_writer,
                         const MetaLog::EntityHeader &header_);

    /** Destructor. */
    virtual ~BalancePlanAuthority() { }

    /** Determines if there are any failover balance plans.
     * This method determines if there are any registered balance plans due to a
     * range server failure.  No failover plans are registered is #m_map is
     * empty
     * @return <i>true</i> if balance plans registered, <i>false</i> otherwise
     */
    bool is_empty();

    /** Creates a recovery plan for a failed range server.
     * When a range server fails, all of its ranges are reassigned to new
     * range servers.  Recovery of ranges needs to happen in the following
     * order:
     *
     *   1. ROOT
     *   2. METADATA
     *   3. SYSTEM
     *   4. USER
     *
     * This method is called by OperationRecover to create and register
     * a balance plan for a failed range server.  This method sets the
     * "removed" bit on the RangeServerConnection object associated with the
     * failed server and persists the modified RangeServerConnection object
     * and this object's state as a single write to the MML.
     * @param location Proxy name for failed range server
     * @param root_specs Vector holding ROOT range spec
     * @param root_states Vector holding ROOT range state
     * @param metadata_specs Vector holding METADATA range specs
     * @param metadata_states Vector holding METADATA range states
     * @param system_specs Vector holding SYSTEM range specs
     * @param system_states Vector holding SYSTEM range states
     * @param user_specs Vector holding USER range specs
     * @param user_states Vector holding USER range states
     */
    void create_recovery_plan(const String &location,
                              const vector<QualifiedRangeSpec> &root_specs,
                              const vector<RangeState> &root_states,
                              const vector<QualifiedRangeSpec> &metadata_specs,
                              const vector<RangeState> &metadata_states,
                              const vector<QualifiedRangeSpec> &system_specs, 
                              const vector<RangeState> &system_states,
                              const vector<QualifiedRangeSpec> &user_specs,
                              const vector<RangeState> &user_states);

    /** Copies part of a recovery plan for a failed range server.
     * @param location Proxy name of failed server
     * @param type Range type indicating portion of recovery plan to copy
     * (see RangeSpec::Type).
     * @param out Out parameter to hold copy of recovery plan
     * @param plan_generation Out parameter filled in with current generation
     * number
     */
    void copy_recovery_plan(const String &location, int type,
                            RangeServerRecovery::Plan &out, int &plan_generation);
    
    /** Removes a recovery plan for a failed range server.
     * This method removes the recovery plan associated with server
     * <code>location</code> and persists the new state in the MML.
     * This method is called after recovery is finished for
     * <code>location</code>.
     * @param location Proxy name of plan to remove
     */
    void remove_recovery_plan(const String &location);

    /** Removes ranges from a failover plan.
     * This method removes the set of ranges specified by <code>ranges</code>
     * from the failover plan for server <code>location</code>.  Once the
     * ranges have been moved, the new state is persisted to the MML.
     * @param location Proxy name identifying failover plan
     * @param type Type of failover plan (see RangeSpec::Type)
     * @param ranges Vector of ranges to remove from plan.
     */
    void remove_from_receiver_plan(const String &location, int type,
                                   const vector<QualifiedRangeSpec> &ranges);

    /** Removes range move specifications for a table.
     * This method removes all of the move specifications for the table
     * identified by <code>table_id</code> from all of the failover
     * plans.
     * @param table_id %Table identifier of move specs to remove
     */
    void remove_table_from_receiver_plan(const String &table_id);

    /** Modifies all failover plans by changing moves to a given
     * destination to a new one.
     * @param location Proxy name of server identifying recovery plan
     *                 ("*" implies all recovery plans)
     * @param type Type of failover plan to modify (see RangeSpec::Type)
     * @param old_destination Existing destination to replace
     * @param new_destination New destination to replace
     * <code>old_destination</code>
     */
    void change_receiver_plan_location(const String &location, int type,
                                       const String &old_destination,
                                       const String &new_destination);

    /** Returns the list of receiver location for a recovery plan.
     * @param location Proxy name of server identifying recovery plan
     * @param type Type of recovery plan (see RangeSpec::Type)
     * @param locations Output parameter filled in with recceiver locations
     */
    void get_receiver_plan_locations(const String &location, int type,
                                     StringSet &locations);
    
    /** Checks if recovery plan of given type has been removed.
     * @param location Proxy name of server identifying recovery plan
     * @param type Type of recovery plan (see RangeSpec::Type)
     * @return <i>true</i> if recovery plan for <code>location</code>,
     * <code>type</code> has been removed, <i>false</i> otherwise.
     */
    bool recovery_complete(const String &location, int type);

    /** Returns the current generation number.
     * The generation number (#m_generation) starts out at zero and is
     * incremented each time a new plan is created for a failed range server.
     * @return Current generation number.
     */
    int get_generation() { std::lock_guard<std::mutex> lock(m_mutex); return m_generation; }

    /** Sets the generation number (TESTING ONLY).
     * @param new_generation New generation number
     */
    void set_generation(int new_generation) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_generation = new_generation;
    }

    /** Clears the #m_current_set of move specifications (TESTING ONLY).
     */
    void clear_current_set() {
      m_current_set.clear();
    }

    /** Registers a new balance plan for load balancing purposes.
     * This method registers the balance plan <code>plan</code> by adding
     * all of the moves in <code>plan</code>to #m_current_set.  It only
     * registers the plan if <code>generation</code> matches the current
     * generation number.  After successfully registering the balance plan, the
     * object state is persisted to the MML along with the entities supplied
     * in <code>entities</code>.
     * @param plan Balance plan to register
     * @param generation Required generation number
     * @param entities Additional entities to persist in MML.
     * @return <i>true</i> if balance plan was registered, <i>false</i>
     * if balance plan was not registered due to generation mismatch.
     */
    bool register_balance_plan(BalancePlanPtr &plan, int generation,
                               std::vector<MetaLog::EntityPtr> &entities);

    /** Returns the balance plan destination for a given range.
     * This method looks up the range identified by <code>table</code> and
     * <code>range</code> in #m_current_set and returns the destination.
     * If the range does not exist in #m_current_set, a new destination
     * is chosen and a new entry is added to #m_current_set.  If
     * #m_current_set was modified, the object state is persisted to the
     * MML
     * @param table %Table identifier of range to look up
     * @param range %RangeSpec of range to lookup
     * @param location Output parameter to hold destination
     * @return <i>false</i> if destination not already assigned and there
     * are no available servers to assign it to, otherwise <i>true</i>
     */
    bool get_balance_destination(const TableIdentifier &table,
                                 const RangeSpec &range, String &location);

    /** Signals that range move is complete.
     * This method removes the range identified by <code>table</code> and
     * <code>range</code> from #m_current_set.
     * @param table %Table identifer of range
     * @param range %RangeSpec identifying range
     */
    void balance_move_complete(const TableIdentifier &table,
                               const RangeSpec &range);

    /** Sets the MML writer
     * Sets #m_mml_writer to <code>mml_writer</code>
     * @param mml_writer Smart pointer to MML writer
     */
    void set_mml_writer(MetaLog::WriterPtr &mml_writer);

    /** Returns the name of this entity ("BalancePlanAuthority")
     * @return Entity name
     */
    virtual const String name() { return "BalancePlanAuthority"; }

    /** Writes a human-readable represenation of object to an output stream.
     * This method writes a human-readable represenation of the object to the
     * output stream <code>os</code>.  The string written starts with the
     * generation number and is followed by all of the failover plans.
     * @param os Output stream to write to
     */
    virtual void display(std::ostream &os);

    /** Reads serialized encoding of object.
     * This method restores the state of the object by decoding a serialized
     * representation of the state from the memory location pointed to by
     * <code>*bufp</code>.  See encode() for a description of the
     * serialized format.
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by
     * <code>*bufp</code> (decremented by call).
     * @param definition_version Version of DefinitionMaster
     */
    void decode(const uint8_t **bufp, size_t *remainp,
                uint16_t definition_version) override;

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    void decode_old(const uint8_t **bufp, size_t *remainp);

    /** Creates a recovery plan for a failed server.
     * Creates a new recovery plan for a failed server for the ranges
     * of type <code>type</code>.  It allocates a RangeServerRecovery::Plan
     * object and adds <code>specs</code> and <code>states</code> to the
     * receiver plan and assigns destinations by walking #m_active in
     * round-robin fashion.  It also populates the replay plan by
     * reading the list of fragments of the failed servers commit log
     * 
     * /hypertable/servers/<code>location</code>/log/<code>type</code>
     * 
     * and assigns each fragment to a server by walking #m_active in
     * round-robin fashion.
     * @param location Proxy name of failed server
     * @param type Type of recovery plan (see RangeSpec::Type)
     * @param specs Vector of ranges to be moved
     * @param states Vector of range states (parallels <code>specs</code>)
     * @return Recovery plan
     */
    RangeServerRecovery::PlanPtr create_range_plan(const String &location, int type,
                                                   const vector<QualifiedRangeSpec> &specs,
                                                   const vector<RangeState> &states);

    /** Modifies recovery plan, replacing moves to <code>location</code> with a
     * new destination.
     * This method modifies the given recovery plan by replacing references to
     * <code>location</code> with other active servers.  It is called by
     * create_recovery_plan() to replace the failed server identified by
     * <code>location</code> from existing recovery plans.
     * @param plan Recovery plan to modify
     * @param location Proxy name of server being recovered
     * @param new_specs Vector of QualifiedRangeSpec objects to remove from
     *        receiver plan
     */
    void update_range_plan(RangeServerRecovery::PlanPtr &plan, const String &location,
                           const vector<QualifiedRangeSpec> &new_specs);

    /// Pointer to master context
    ContextPtr m_context;

    /// Pointer to MML writer
    MetaLog::WriterPtr m_mml_writer;

    /// Generation number (incremented with each new failover plan)
    int m_generation;

    /// Cache of active (available) servers
    StringSet m_active;

    /// Iterator pointing into #m_active
    StringSet::iterator m_active_iter;

    /** Holds plans for each range type.
     * This structure holds recovery plans for each of the four types of ranges,
     * ROOT, METADATA, SYSTEM, and USER (see RangeSpec::Type).
     */
    struct RecoveryPlans {
      RangeServerRecovery::PlanPtr plans[4];
    };

    /// Server-to-plan map
    typedef std::map<String, RecoveryPlans> RecoveryPlanMap;

    /// Mapping from failed range server to recovery plan
    RecoveryPlanMap m_map;

    /** <a href="http://www.sgi.com/tech/stl/StrictWeakOrdering.html">StrictWeakOrdering</a>
     * class for set of RangeMoveSpecs.
     */ 
    struct lt_move_spec {
      bool operator()(const RangeMoveSpecPtr &ms1,
                      const RangeMoveSpecPtr &ms2) const  {
        int cmp = strcmp(ms1->table.id, ms2->table.id);
        if (cmp < 0)
          return true;
        else if (cmp == 0) {
          if (ms1->range < ms2->range)
            return true;
        }
        return false;
      }
    };

    /// Set of RangeMoveSpecPtr
    typedef std::set<RangeMoveSpecPtr, lt_move_spec> MoveSetT;

    /// Current set of move specifications for move operations
    MoveSetT m_current_set;
  };

  /// Smart pointer to BalancePlanAuthority
  typedef std::shared_ptr<BalancePlanAuthority> BalancePlanAuthorityPtr;

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_BALANCEPLANAUTHORITY_H

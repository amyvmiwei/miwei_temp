/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_BALANCEPLANAUTHORITY_H
#define HYPERTABLE_BALANCEPLANAUTHORITY_H

#include <map>

#include <boost/thread/condition.hpp>

#include "Hypertable/Lib/BalancePlan.h"
#include "Hypertable/Lib/MetaLogWriter.h"
#include "Hypertable/Lib/RangeRecoveryPlan.h"

#include "Context.h"

namespace Hypertable {

  /**
   * BalancePlanAuthority is a class that acts as a central authority for all
   * balance plans created due to auto-balancing (???) and range server failover
   */
  class BalancePlanAuthority : public MetaLog::Entity {
  public:
    BalancePlanAuthority(ContextPtr context, MetaLog::WriterPtr &mml_writer);

    BalancePlanAuthority(ContextPtr context, MetaLog::WriterPtr &mml_writer,
            const MetaLog::EntityHeader &header_);

    virtual ~BalancePlanAuthority() { }

    // returns true if there are no pending balance plans
    /**
     * Determines if there are any balance plans registered due to a
     * range server failure
     *
     * @return <i>true</i> if balance plans registered, <i>false</i> otherwiseb
     */
    bool is_empty();

    // creates a new recovery plan; invoked by OperationRecoverRanges
    void create_recovery_plan(const String &location,
                              const vector<QualifiedRangeSpec> &root_specs,
                              const vector<RangeState> &root_states,
                              const vector<QualifiedRangeSpec> &metadata_specs,
                              const vector<RangeState> &metadata_states,
                              const vector<QualifiedRangeSpec> &system_specs, 
                              const vector<RangeState> &system_states,
                              const vector<QualifiedRangeSpec> &user_specs,
                              const vector<RangeState> &user_states);

    // Copies a recovery plan
    void copy_recovery_plan(const String &location, int type,
                            RangeRecoveryPlan &out, int &plan_generation);

    // deletes a recovery plan; call this after recovery finished
    void remove_recovery_plan(const String &location);

    void remove_from_receiver_plan(const String &location, int type,
                                   const vector<QualifiedRangeSpec> &ranges,
                                   std::vector<Entity *> &entities);

    void remove_from_receiver_plan(const String &location, int type,
                                   const vector<QualifiedRangeSpec> &ranges);

    void remove_from_replay_plan(const String &recovery_location, int type,
                                 const String &replay_location);

    void get_receiver_plan_locations(const String &location, int type,
                                     StringSet &locations);

    bool recovery_complete(const String &location, int type);

    // returns the plan generation; increased whenever a RangeServer fails
    // and a new recovery plan is added
    int get_generation() { ScopedLock lock(m_mutex); return m_generation; }

    // returns true if there are any MoveRange operations in the plan
    bool has_plan_moves() {
      ScopedLock lock(m_mutex);
      return m_current_set.size() > 0;
    }

    // registers a new BalancePlan
    bool register_balance_plan(BalancePlanPtr &plan, int generation,
                               std::vector<Entity *> &entities);

    // returns the new destination of a range; used by OperationMoveRange
    bool get_balance_destination(const TableIdentifier &table,
                    const RangeSpec &range, String &location);

    // signals that the move of this range is complete
    bool balance_move_complete(const TableIdentifier &table,
                    const RangeSpec &range, int32_t error = 0);


    virtual const String name() { return "BalancePlanAuthority"; }
    virtual void display(std::ostream &os);
    virtual size_t encoded_length() const;
    virtual void encode(uint8_t **bufp) const;
    virtual void decode(const uint8_t **bufp, size_t *remainp);
    virtual void set_mml_writer(MetaLog::WriterPtr &mml_writer);

  private:
    RangeRecoveryPlan *create_range_plan(const String &location, int type,
                                         const vector<QualifiedRangeSpec> &specs,
                                         const vector<RangeState> &states);

    /**
     * This method modifies the given recovery plan by replacing
     * references to <code>location</code> with other active
     * servers.
     *
     * @param plan recovery plan for (range-server, commit-log-type) combo
     * @param location proxy name of server being recovered
     * @param new_specs vector of QualifiedRangeSpec objects to removed from
     *        receiver plan
     */
    void update_range_plan(RangeRecoveryPlanPtr &plan, const String &location,
                           const vector<QualifiedRangeSpec> &new_specs);

    Mutex m_mutex;
    ContextPtr m_context;
    MetaLog::WriterPtr m_mml_writer;
    int m_generation;
    StringSet m_active;
    StringSet::iterator m_active_iter;

    struct RecoveryPlans {
      RangeRecoveryPlanPtr plans[4];
    };

    typedef std::map<String, RecoveryPlans> RecoveryPlanMap;
    RecoveryPlanMap m_map;

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
    typedef std::set<RangeMoveSpecPtr, lt_move_spec> MoveSetT;

    MoveSetT m_current_set;
  };

  typedef intrusive_ptr<BalancePlanAuthority> BalancePlanAuthorityPtr;

} // namespace Hypertable

#endif // HYPERTABLE_BALANCEPLANAUTHORITY_H

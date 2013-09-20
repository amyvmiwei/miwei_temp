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

#ifndef HYPERTABLE_OPERATIONRECOVER_H
#define HYPERTABLE_OPERATIONRECOVER_H

#include <vector>

#include "Common/PageArenaAllocator.h"

#include "Hypertable/Lib/Types.h"
#include "Hypertable/RangeServer/MetaLogEntityRange.h"

#include "Operation.h"
#include "RangeServerConnection.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Carries out recovery operaton for a range server.
   */
  class OperationRecover : public Operation {
  public:

    enum {
      RESTART = 1
    };

    /** Constructor.
     * This method constructs a new OperationRecover object.  If #RESTART is
     * passed in for the <code>flags</code> argument then it will prevent
     * notifiction if unable to acquire lock on range server's lock file.
     * This is expected behavior on service restart, so no notification
     * should be deliverd.
     * @param context Master context object
     * @param rsc RangeServerConnection object referring to server to be
     * recovered
     * @param flags Set to #RESTART if this object is being created due to a
     * server restart
     */
    OperationRecover(ContextPtr &context, RangeServerConnectionPtr &rsc,
                     int flags=0);

    OperationRecover(ContextPtr &context, const MetaLog::EntityHeader &header_);

    virtual ~OperationRecover();

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual const String location() { return m_location; }

    virtual void display_state(std::ostream &os);
    virtual uint16_t encoding_version() const;
    virtual size_t encoded_state_length() const;
    virtual void encode_state(uint8_t **bufp) const;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);
    virtual bool exclusive() { return true; }

  private:

    // acquire lock on Hyperspace file; returns true if lock is acquired or
    // false if the RangeServer is back online
    bool acquire_server_lock();

    // creates a new recovery plan and stores it in the BalancePlanAuthority
    void create_recovery_plan();

    // read rsml files and populate m_root_range, m_metadata_ranges etc
    void read_rsml();

    // check to see if master was notified of newly split-off range
    void handle_split_shrunk(MetaLogEntityRange *range_entity);

    // cleans up after this operation is complete
    void clear_server_state();

    // persisted state
    String m_location;
    vector<QualifiedRangeSpec> m_root_specs;
    vector<RangeState> m_root_states;
    vector<QualifiedRangeSpec> m_metadata_specs;
    vector<RangeState> m_metadata_states;
    vector<QualifiedRangeSpec> m_system_specs;
    vector<RangeState> m_system_states;
    vector<QualifiedRangeSpec> m_user_specs;
    vector<RangeState> m_user_states;
    // in mem state
    CharArena m_arena;
    RangeServerConnectionPtr m_rsc;
    String m_subop_dependency;
    String m_hostname;
    uint64_t m_hyperspace_handle;
    bool m_restart;
    bool m_lock_acquired;
  };

  /** @}*/

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONRECOVER_H

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
#include "EntityType.h"

namespace Hypertable {

  namespace OperationState {
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
    const char *get_text(int state);
  }

  namespace Dependency {
    extern const char *INIT;
    extern const char *SERVERS;
    extern const char *ROOT;
    extern const char *METADATA;
    extern const char *SYSTEM;
    extern const char *RECOVER_SERVER;
    extern const char *RECOVERY_BLOCKER;
    extern const char *RECOVERY;
  }

  namespace NamespaceFlag {
    enum {
      CREATE_INTERMEDIATE = 0x0001,
      IF_EXISTS           = 0x0002,
      IF_NOT_EXISTS       = 0x0004
    };
  }

  typedef std::set<String> DependencySet;

  class Operation : public MetaLog::Entity {
  public:
    Operation(ContextPtr &context, int32_t type);
    Operation(ContextPtr &context, EventPtr &event, int32_t type);
    Operation(ContextPtr &context, const MetaLog::EntityHeader &header_);
    virtual ~Operation() { }

    virtual void execute() = 0;
    virtual const String name() = 0;
    virtual const String label() = 0;
    virtual const String graphviz_label() { return label(); }
    virtual bool exclusive() { return false; }

    virtual void decode_request(const uint8_t **bufp, size_t *remainp) = 0;
    virtual size_t encoded_state_length() const = 0;
    virtual void encode_state(uint8_t **bufp) const = 0;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp) = 0;
    virtual void display_state(std::ostream &os) = 0;

    virtual size_t encoded_result_length() const;
    virtual void encode_result(uint8_t **bufp) const;
    virtual void decode_result(const uint8_t **bufp, size_t *remainp);

    virtual void display_results(std::ostream &os) { }

    virtual size_t encoded_length() const;
    virtual void encode(uint8_t **bufp) const;
    virtual void decode(const uint8_t **bufp, size_t *remainp);

    virtual void display(std::ostream &os);

    int64_t id() { return header.id; }
    HiResTime expiration_time() { ScopedLock lock(m_mutex); return m_expiration_time; }

    virtual bool remove_explicitly() { return false; }
    virtual int32_t remove_approval_mask() { return 0; }
    bool remove_approval_add(int32_t approval) {
      ScopedLock lock(m_mutex);
      m_remove_approvals |= approval;
      return m_remove_approvals == remove_approval_mask();
    }
    bool remove_ok() { ScopedLock lock(m_mutex); return m_remove_approvals == remove_approval_mask(); }

    void complete_error(int error, const String &msg);
    void complete_error_no_log(int error, const String &msg);
    void complete_error(Exception &e);
    void complete_ok();
    void complete_ok_no_log();

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

  protected:
    Mutex m_mutex;
    ContextPtr m_context;
    EventPtr m_event;
    int32_t m_state;
    int32_t m_error;
    int32_t m_remove_approvals;
    int32_t m_original_type;
    bool m_unblock_on_exit;
    bool m_blocked;
    String m_error_msg;
    HiResTime m_expiration_time;
    int64_t m_hash_code;
    DependencySet m_exclusivities;
    DependencySet m_dependencies;
    DependencySet m_obstructions;
    std::vector<Operation *> m_sub_ops;
  };
  typedef intrusive_ptr<Operation> OperationPtr;

} // namespace Hypertable

#endif // HYPERTABLE_OPERATION_H

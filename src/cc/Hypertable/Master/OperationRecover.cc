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

#include <Common/Compat.h>

#include "OperationRecover.h"

#include <Hypertable/Master/BalancePlanAuthority.h>
#include <Hypertable/Master/OperationMoveRange.h>
#include <Hypertable/Master/OperationRecoverRanges.h>
#include <Hypertable/Master/OperationRelinquishAcknowledge.h>
#include <Hypertable/Master/ReferenceManager.h>
#include <Hypertable/Master/Utility.h>

#include <Hypertable/RangeServer/MetaLogDefinitionRangeServer.h>
#include <Hypertable/RangeServer/MetaLogEntityRange.h>
#include <Hypertable/RangeServer/MetaLogEntityTaskAcknowledgeRelinquish.h>

#include <Hypertable/Lib/LegacyDecoder.h>
#include <Hypertable/Lib/MetaLogReader.h>

#include <Common/Error.h>
#include <Common/md5.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>

#include <sstream>

extern "C" {
#include <poll.h>
#include <time.h>
}

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

OperationRecover::OperationRecover(ContextPtr &context, 
                                   RangeServerConnectionPtr &rsc, int flags)
  : Operation(context, MetaLog::EntityType::OPERATION_RECOVER_SERVER),
    m_location(rsc->location()), m_rsc(rsc), m_restart(flags==RESTART) {
  m_dependencies.insert(Dependency::RECOVERY_BLOCKER);
  m_dependencies.insert(Dependency::RECOVERY);
  m_dependencies.insert(String("RegisterServer ") + m_location);
  m_exclusivities.insert(m_rsc->location());
  m_obstructions.insert(Dependency::RECOVER_SERVER);
  m_hash_code = md5_hash("OperationRecover") ^ md5_hash(m_rsc->location().c_str());
  HT_ASSERT(m_rsc != 0);
}

OperationRecover::OperationRecover(ContextPtr &context,
                                   const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}


void OperationRecover::execute() {
  vector<MetaLog::EntityPtr> removable_move_ops;
  int state = get_state();
  String subject, message;

  HT_INFOF("Entering RecoverServer %s state=%s this=%p",
           m_location.c_str(), OperationState::get_text(state), (void *)this);
  if (!m_rsc)
    (void)m_context->rsc_manager->find_server_by_location(m_location, m_rsc);
  else
    HT_ASSERT(m_location == m_rsc->location());

  if (m_hostname.empty() && m_rsc)
    m_hostname = m_rsc->hostname();

  switch (state) {

  case OperationState::INITIAL:
    // Prevent any RegisterServer operations for this server from running while
    // recovery is in progress
    {
      ScopedLock lock(m_mutex);
      String register_server_label = String("RegisterServer ") + m_location;
      m_dependencies.erase(register_server_label);
      m_exclusivities.insert(register_server_label);
      m_state = OperationState::STARTED;
    }
    break;

  case OperationState::STARTED:

    if (!acquire_server_lock()) {
      if (m_rsc)
        m_rsc->set_recovering(false);
      m_expiration_time.reset();  // force it to get removed immediately
      complete_ok();
      return;
    }

    // This shouldn't be necessary, however we've seen Hyperspace declare a
    // server to be dead when it was in fact still alive (see issue 1346).
    // We'll leave this in place until Hyperspace gets overhauled.
    {
      CommAddress addr;
      addr.set_proxy(m_location);
      Utility::shutdown_rangeserver(m_context, addr);
    }

    if (m_rsc) {

      m_rsc->set_recovering(true);

      m_context->rsc_manager->disconnect_server(m_rsc);

      // Remove by public address
      CommAddress comm_addr(m_rsc->public_addr());
      m_context->conn_manager->remove(comm_addr);

      // Remove by location
      comm_addr.set_proxy(m_rsc->location());
      m_context->conn_manager->remove(comm_addr);
    }

    m_context->remove_available_server(m_location);

    // Remove proxy from AsyncComm
    m_context->comm->remove_proxy(m_location);

    // Send notification
    subject = format("NOTICE: Recovery of %s (%s) starting",
                     m_location.c_str(), m_hostname.c_str());
    message = format("Failure of range server %s (%s) has been detected.  "
                     "Starting recovery...", m_location.c_str(),
                     m_hostname.c_str());
    HT_INFOF("%s", message.c_str());
    m_context->notification_hook(subject, message);

    // read rsml figure out what types of ranges lived on this server
    // and populate the various vectors of ranges
    try {
      read_rsml(removable_move_ops);
    }
    catch (Exception &e) {
      subject = format("ERROR: Recovery of %s (%s)",
                       m_location.c_str(), m_hostname.c_str());
      message = format("Problem reading RSML - %s - %s",
                       Error::get_text(e.code()), e.what());
      time_t last_notification = 0;
      while (true) {
        time_t now = time(0);
        int32_t notify_interval = 
          m_context->props->get_i32("Hypertable.Master.NotificationInterval");
        if (last_notification + notify_interval <= now) {
          m_context->notification_hook(subject, message);
          last_notification = time(0);
        }
        HT_ERRORF("%s", message.c_str());
        poll(0, 0, 30000);
      }
    }

    // now create a new recovery plan
    create_recovery_plan();

    set_state(OperationState::ISSUE_REQUESTS);

    HT_MAYBE_FAIL("recover-server-1");
    record_state(removable_move_ops);
    HT_MAYBE_FAIL("recover-server-2");
    break;

  case OperationState::ISSUE_REQUESTS:
    if (m_root_specs.size()) {
      HT_INFOF("Number of root ranges to recover for location %s = %u",
               m_location.c_str(), (unsigned)m_root_specs.size());
      stage_subop(make_shared<OperationRecoverRanges>(m_context, m_location,
                                                      RangeSpec::ROOT));
    }
    if (m_metadata_specs.size()) {
      HT_INFOF("Number of metadata ranges to recover for location %s = %u",
               m_location.c_str(), (unsigned)m_metadata_specs.size());
      stage_subop(make_shared<OperationRecoverRanges>(m_context, m_location,
                                                      RangeSpec::METADATA));
    }
    if (m_system_specs.size()) {
      HT_INFOF("Number of system ranges to recover for location %s = %d",
               m_location.c_str(), (int)m_system_specs.size());
      stage_subop(make_shared<OperationRecoverRanges>(m_context, m_location,
                                                      RangeSpec::SYSTEM));
    }
    if (m_user_specs.size()) {
      HT_INFOF("Number of user ranges to recover for location %s = %d",
               m_location.c_str(), (int)m_user_specs.size());
      stage_subop(make_shared<OperationRecoverRanges>(m_context, m_location,
                                                      RangeSpec::USER));
    }
    set_state(OperationState::FINALIZE);
    record_state();
    HT_MAYBE_FAIL("recover-server-3");
    break;

  case OperationState::FINALIZE:

    if (!validate_subops())
      break;

    // Once recovery is complete, the master blows away the RSML and CL for the
    // server being recovered then it unlocks the hyperspace file
    clear_server_state();

    HT_MAYBE_FAIL("recover-server-4");

    m_expiration_time.reset();  // force it to get removed immediately

    if (m_rsc) {
      std::vector<MetaLog::EntityPtr> additional;
      m_rsc->mark_for_removal();
      additional.push_back(m_rsc);
      complete_ok(additional);
    }
    else
      complete_ok();

    // Send notification
    subject = format("NOTICE: Recovery of %s (%s) succeeded",
                     m_location.c_str(), m_hostname.c_str());
    message = format("Recovery of range server %s (%s) has succeeded.",
                     m_location.c_str(), m_hostname.c_str());
    m_context->notification_hook(subject, message);
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
    break;
  }

  HT_INFOF("Leaving RecoverServer %s state=%s this=%p",
           m_location.c_str(), OperationState::get_text(get_state()), 
           (void *)this);
}

OperationRecover::~OperationRecover() {
}


bool OperationRecover::acquire_server_lock() {
  String subject, message;

  if (m_lock_acquired)
    return true;

  try {
    String fname = m_context->toplevel_dir + "/servers/" + m_location;
    uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
    LockStatus lock_status = LOCK_STATUS_BUSY;
    LockSequencer sequencer;
    uint64_t handle = 0;
    
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, &handle);

    handle = m_context->hyperspace->open(fname, oflags);

    m_context->hyperspace->try_lock(handle, 
                                    LOCK_MODE_EXCLUSIVE, &lock_status,
                                    &sequencer);
    if (lock_status != LOCK_STATUS_GRANTED) {
      HT_INFOF("Couldn't obtain lock on '%s' due to conflict, lock_status=0x%x",
               fname.c_str(), lock_status);
      if (!m_restart) {
        // Send notification
        subject = format("NOTICE: Recovery of %s (%s) aborted",
                         m_location.c_str(), m_hostname.c_str());
        message = format("Aborting recovery of range server %s (%s) because "
                         "unable to aquire lock.", m_location.c_str(),
                         m_hostname.c_str());
        HT_INFOF("%s", message.c_str());
        m_context->notification_hook(subject, message);
      }
      else
        m_restart = false;
      return false;
    }

    m_context->hyperspace->attr_set(handle, "removed", "", 0);

    m_hyperspace_handle = handle;
    handle = 0;
    m_lock_acquired = true;

    HT_INFOF("Acquired lock on '%s', starting recovery...", fname.c_str());
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Problem obtaining " << m_location 
                 << " hyperspace lock (" << e << "), aborting..." << HT_END;
    return false;
  }
  return true;
}

void OperationRecover::display_state(std::ostream &os) {
  os << " location=" << m_location << " ";
}

const String OperationRecover::name() {
  return label();
}

const String OperationRecover::label() {
  return format("OperationRecover %s", m_location.c_str());
}

void OperationRecover::clear_server_state() {

  // if m_rsc is NULL then it was already removed
  if (m_rsc)
    m_context->monitoring->drop_server(m_rsc->location());

  // unlock hyperspace file
  Hyperspace::close_handle_ptr(m_context->hyperspace, &m_hyperspace_handle);
  // delete balance plan
  BalancePlanAuthority *plan = m_context->get_balance_plan_authority();
  plan->remove_recovery_plan(m_location);
}

void OperationRecover::create_recovery_plan() {
  BalancePlanAuthority *plan = m_context->get_balance_plan_authority();
  plan->create_recovery_plan(m_location,
                             m_root_specs, m_root_states,
                             m_metadata_specs, m_metadata_states,
                             m_system_specs, m_system_states,
                             m_user_specs, m_user_states);
}

 void OperationRecover::read_rsml(vector<MetaLog::EntityPtr> &removable_move_ops) {
  // move rsml and commit log to some recovered dir
  MetaLog::DefinitionPtr rsml_definition
      = new MetaLog::DefinitionRangeServer(m_location.c_str());  
  MetaLogEntityRange *range;
  MetaLog::EntityTaskAcknowledgeRelinquish *ack_task;
  vector<MetaLog::EntityPtr> entities;
  StringSet missing_tables, valid_tables;
  String tablename;
  String logfile = m_context->toplevel_dir + "/servers/" + m_location + "/log/"
    + rsml_definition->name();
  MetaLog::ReaderPtr rsml_reader = 
    new MetaLog::Reader(m_context->dfs, rsml_definition, logfile);

  rsml_reader->get_entities(entities);

  try {
    foreach_ht (MetaLog::EntityPtr &entity, entities) {
      if ((range = dynamic_cast<MetaLogEntityRange *>(entity.get())) != 0) {
        QualifiedRangeSpec spec;
        RangeStateManaged range_state;
        RangeSpecManaged range_spec;
        std::stringstream sout;

        // skip phantom ranges, let whoever was recovering them deal with them
        if (range->get_state() & RangeState::PHANTOM) {
          sout << "Skipping PHANTOM range " << *range;
          HT_INFOF("%s", sout.str().c_str());
        }
        else {
	  TableIdentifier table;

	  // Check for missing table
          range->get_table_identifier(table);
	  if (!table.is_system() && valid_tables.count(table.id) == 0) {
	    if (missing_tables.count(table.id) == 0) {
	      if (!m_context->namemap->id_to_name(table.id, tablename))
		missing_tables.insert(table.id);
	      else
		valid_tables.insert(table.id);
	    }
	    if (missing_tables.count(table.id) != 0) {
	      sout << "Range " << *range << ": table does not exist, skipping ...";
	      HT_WARNF("%s", sout.str().c_str());
	      continue;
	    }
	  }

          sout << "Range " << *range << ": not PHANTOM; including";
          HT_INFOF("%s", sout.str().c_str());

          if (range->get_state() == RangeState::SPLIT_SHRUNK)
            handle_split_shrunk(range, removable_move_ops);

          range->get_table_identifier(spec.table);
          range->get_range_spec(range_spec);
          range->get_range_state(range_state);

          spec.range = range_spec;

          if (spec.is_root()) {
            m_root_specs.push_back(QualifiedRangeSpec(m_arena, spec));
            m_root_states.push_back(RangeState(m_arena, range_state));
          }
          else if (spec.table.is_metadata()) {
            m_metadata_specs.push_back(QualifiedRangeSpec(m_arena, spec));
            m_metadata_states.push_back(RangeState(m_arena, range_state));
          }
          else if (spec.table.is_system()) {
            m_system_specs.push_back(QualifiedRangeSpec(m_arena, spec));
            m_system_states.push_back(RangeState(m_arena, range_state));
          }
          else {
            m_user_specs.push_back(QualifiedRangeSpec(m_arena, spec));
            m_user_states.push_back(RangeState(m_arena, range_state));
          }
        }
      }
      else if ((ack_task = dynamic_cast<MetaLog::EntityTaskAcknowledgeRelinquish *>(entity.get())) != 0) {
        OperationPtr operation =
          make_shared<OperationRelinquishAcknowledge>(m_context,
                                                      ack_task->location,
                                                      ack_task->range_id,
                                                      ack_task->table,
                                                      ack_task->range_spec);
        operation->execute();
      }
    }
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }
}


void OperationRecover::handle_split_shrunk(MetaLogEntityRange *range_entity,
                                           vector<MetaLog::EntityPtr> &removable_move_ops) {
  String start_row, end_row;
  String split_row = range_entity->get_split_row();
  String old_boundary_row = range_entity->get_old_boundary_row();
  bool split_off_high = split_row.compare(old_boundary_row) < 0;

  TableIdentifier table;
  RangeSpecManaged range;

  range_entity->get_table_identifier(table);
  range_entity->get_boundary_rows(start_row, end_row);

  if (split_off_high) {
    range.set_start_row(end_row);
    range.set_end_row(old_boundary_row);
  }
  else {
    range.set_start_row(old_boundary_row);
    range.set_end_row(start_row);
  }

  int64_t hash_code
    = OperationMoveRange::hash_code(table, range, range_entity->get_source(),
                                    range_entity->id());

  OperationPtr operation = m_context->get_move_operation(hash_code);
  if (operation) {
    if (operation->remove_approval_add(0x01)) {
      m_context->remove_move_operation(operation);
      removable_move_ops.push_back(operation);
    }
    int64_t soft_limit = range_entity->get_soft_limit();
    range_entity->clear_state();
    range_entity->set_soft_limit(soft_limit);
  }
  
}

uint8_t OperationRecover::encoding_version_state() const {
  return 1;
}

size_t OperationRecover::encoded_length_state() const {
  size_t len = Serialization::encoded_length_vstr(m_location) + 16;
  for (size_t i=0; i<m_root_specs.size(); i++)
    len += m_root_specs[i].encoded_length() + m_root_states[i].encoded_length();
  for (size_t i=0; i<m_metadata_specs.size(); i++)
    len += m_metadata_specs[i].encoded_length() + m_metadata_states[i].encoded_length();
  for (size_t i=0; i<m_system_specs.size(); i++)
    len += m_system_specs[i].encoded_length() + m_system_states[i].encoded_length();
  for (size_t i=0; i<m_user_specs.size(); i++)
    len += m_user_specs[i].encoded_length() + m_user_states[i].encoded_length();
  return len;
}

void OperationRecover::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_location);
  // root
  Serialization::encode_i32(bufp, m_root_specs.size());
  for (size_t i=0; i<m_root_specs.size(); i++) {
    m_root_specs[i].encode(bufp);
    m_root_states[i].encode(bufp);
  }
  // metadata
  Serialization::encode_i32(bufp, m_metadata_specs.size());
  for (size_t i=0; i<m_metadata_specs.size(); i++) {
    m_metadata_specs[i].encode(bufp);
    m_metadata_states[i].encode(bufp);
  }
  // system
  Serialization::encode_i32(bufp, m_system_specs.size());
  for (size_t i=0; i<m_system_specs.size(); i++) {
    m_system_specs[i].encode(bufp);
    m_system_states[i].encode(bufp);
  }
  // user
  Serialization::encode_i32(bufp, m_user_specs.size());
  for (size_t i=0; i<m_user_specs.size(); i++) {
    m_user_specs[i].encode(bufp);
    m_user_states[i].encode(bufp);
  }
}

void OperationRecover::decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  m_location = Serialization::decode_vstr(bufp, remainp);
  int nn;
  QualifiedRangeSpec spec;
  RangeState state;
  nn = Serialization::decode_i32(bufp, remainp);
  for (int ii = 0; ii < nn; ++ii) {
    spec.decode(bufp, remainp);
    m_root_specs.push_back(QualifiedRangeSpec(m_arena, spec));
    state.decode(bufp, remainp);
    m_root_states.push_back(RangeState(m_arena, state));
  }
  nn = Serialization::decode_i32(bufp, remainp);
  for (int ii = 0; ii < nn; ++ii) {
    spec.decode(bufp, remainp);
    m_metadata_specs.push_back(QualifiedRangeSpec(m_arena, spec));
    state.decode(bufp, remainp);
    m_metadata_states.push_back(RangeState(m_arena, state));
  }
  nn = Serialization::decode_i32(bufp, remainp);
  for (int ii = 0; ii < nn; ++ii) {
    spec.decode(bufp, remainp);
    m_system_specs.push_back(QualifiedRangeSpec(m_arena, spec));
    state.decode(bufp, remainp);
    m_system_states.push_back(RangeState(m_arena, state));
  }
  nn = Serialization::decode_i32(bufp, remainp);
  for (int ii = 0; ii < nn; ++ii) {
    spec.decode(bufp, remainp);
    m_user_specs.push_back(QualifiedRangeSpec(m_arena, spec));
    state.decode(bufp, remainp);
    m_user_states.push_back(RangeState(m_arena, state));
  }
}

void OperationRecover::decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  if (version == 0)
    Serialization::decode_i16(bufp, remainp);  // skip old version
  m_location = Serialization::decode_vstr(bufp, remainp);
  int nn;
  QualifiedRangeSpec spec;
  RangeState state;
  nn = Serialization::decode_i32(bufp, remainp);
  for (int ii = 0; ii < nn; ++ii) {
    legacy_decode(bufp, remainp, &spec);
    m_root_specs.push_back(QualifiedRangeSpec(m_arena, spec));
    legacy_decode(bufp, remainp, &state);
    m_root_states.push_back(RangeState(m_arena, state));
  }
  nn = Serialization::decode_i32(bufp, remainp);
  for (int ii = 0; ii < nn; ++ii) {
    legacy_decode(bufp, remainp, &spec);
    m_metadata_specs.push_back(QualifiedRangeSpec(m_arena, spec));
    legacy_decode(bufp, remainp, &state);
    m_metadata_states.push_back(RangeState(m_arena, state));
  }
  nn = Serialization::decode_i32(bufp, remainp);
  for (int ii = 0; ii < nn; ++ii) {
    legacy_decode(bufp, remainp, &spec);
    m_system_specs.push_back(QualifiedRangeSpec(m_arena, spec));
    legacy_decode(bufp, remainp, &state);
    m_system_states.push_back(RangeState(m_arena, state));
  }
  nn = Serialization::decode_i32(bufp, remainp);
  for (int ii = 0; ii < nn; ++ii) {
    legacy_decode(bufp, remainp, &spec);
    m_user_specs.push_back(QualifiedRangeSpec(m_arena, spec));
    legacy_decode(bufp, remainp, &state);
    m_user_states.push_back(RangeState(m_arena, state));
  }
}

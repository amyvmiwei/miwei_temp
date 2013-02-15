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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/md5.h"
#include "Common/PageArenaAllocator.h"
#include "Common/FailureInducer.h"

#include "Hypertable/Lib/CommitLogReader.h"

#include "BalancePlanAuthority.h"
#include "OperationMoveRange.h"
#include "OperationRecoverRanges.h"
#include "OperationRecoveryBlocker.h"
#include "OperationProcessor.h"
#include "Utility.h"

#include <algorithm>
#include <iostream>
#include <set>

#define OPERATION_RECOVER_RANGES_VERSION 1

using namespace Hypertable;

OperationRecoverRanges::OperationRecoverRanges(ContextPtr &context,
        const String &location, int type, const String &parent_dependency)
  : Operation(context, MetaLog::EntityType::OPERATION_RECOVER_SERVER_RANGES),
    m_location(location), m_parent_dependency(parent_dependency),
    m_type(type), m_plan_generation(0), m_last_notification(0) {
  HT_ASSERT(type != RangeSpec::UNKNOWN);
  initialize_obstructions_dependencies();
}

OperationRecoverRanges::OperationRecoverRanges(ContextPtr &context,
                                    const MetaLog::EntityHeader &header_) 
  : Operation(context, header_), m_plan_generation(0), m_last_notification(0) {
}

void OperationRecoverRanges::execute() {
  int state = get_state();

  HT_INFOF("Entering RecoverServerRanges (%p) %s type=%d plan_generation=%d state=%s",
          (void *)this, m_location.c_str(), m_type, m_plan_generation,
          OperationState::get_text(state));

  if (!m_context->recovery_state().get_replay_future(id())) {
    m_timeout = m_context->props->get_i32("Hypertable.Failover.Timeout");
    set_type_str();
    if (get_new_recovery_plan())
      return;
    create_futures();
  }

  switch (state) {
  case OperationState::INITIAL:

    if (get_new_recovery_plan())
      return;

    // if there are no ranges then there is nothing to do
    if (m_plan.receiver_plan.empty()) {
      HT_INFOF("Plan for location %s, type %s is empty, nothing to do",
               m_location.c_str(), m_type_str.c_str());
      complete_ok();
      break;
    }

    HT_MAYBE_FAIL(format("recover-server-ranges-%s-initial-1", m_type_str.c_str()));
    set_state(OperationState::PHANTOM_LOAD);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-initial-2", m_type_str.c_str()));

    // fall through

  case OperationState::PHANTOM_LOAD:

    if (!wait_for_quorum())
      break;

    if (recovery_plan_has_changed()) {
      set_state(OperationState::INITIAL);
      m_context->mml_writer->record_state(this);
      break;
    }

    try {
      if (!phantom_load_ranges()) {
        // repeat phantom load
        HT_MAYBE_FAIL(format("recover-server-ranges-%s-load-2",
                             m_type_str.c_str()));
        break;
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      HT_THROW(e.code(), e.what());
    }
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-load-3", m_type_str.c_str()));
    set_state(OperationState::REPLAY_FRAGMENTS);
    m_context->mml_writer->record_state(this);

    // fall through to replay fragments

  case OperationState::REPLAY_FRAGMENTS:

    if (!wait_for_quorum())
      break;

    if (recovery_plan_has_changed()) {
      set_state(OperationState::INITIAL);
      break;
    }

    try {
      if (!replay_fragments()) {
        HT_MAYBE_FAIL(format("recover-server-ranges-%s-replay-2", m_type_str.c_str()));
        break;
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      HT_THROW(e.code(), e.what());
    }
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-replay-3", m_type_str.c_str()));
    set_state(OperationState::PREPARE);
    m_context->mml_writer->record_state(this);

    // fall through to prepare

  case OperationState::PREPARE:

    if (!wait_for_quorum())
      break;

    if (recovery_plan_has_changed()) {
      set_state(OperationState::INITIAL);
      break;
    }

    try {
      // tell destination servers to merge fragment data into range,
      // link in transfer logs to commit log
      if (!prepare_to_commit()) {
        HT_MAYBE_FAIL(format("recover-server-ranges-%s-prepare-2", m_type_str.c_str()));
        break;
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      HT_THROW(e.code(), e.what());
    }
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-prepare-3", m_type_str.c_str()));
    set_state(OperationState::COMMIT);
    m_context->mml_writer->record_state(this);

    // fall through to commit

  case OperationState::COMMIT:

    if (!wait_for_quorum())
      break;

    if (!commit()) {
      // repeat commit
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-commit-2", m_type_str.c_str()));
      break;
    }
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-commit-3", m_type_str.c_str()));
    set_state(OperationState::ACKNOWLEDGE);
    m_context->mml_writer->record_state(this);

    // fall through

  case OperationState::ACKNOWLEDGE:

    if (!wait_for_quorum())
      break;

    if (!acknowledge()) {
      // wait a few seconds and then try again
      poll(0, 0, 5000);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-12", m_type_str.c_str()));
      break;
    }
    HT_MAYBE_FAIL(format("recover-server-ranges-%s-ack-3", m_type_str.c_str()));

    if (recovery_plan_has_changed() || !m_redo_set.empty()) {
      m_redo_set.clear();
      set_state(OperationState::INITIAL);
      break;
    }

    HT_ASSERT(m_context->get_balance_plan_authority()->recovery_complete(m_location, m_type));
      
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
    break;
  }

  HT_INFOF("Leaving RecoverServerRanges %s plan_generation=%d type=%d state=%s",
          m_location.c_str(), m_plan_generation, m_type,
          OperationState::get_text(get_state()));
}

void OperationRecoverRanges::display_state(std::ostream &os) {
  os << " location=" << m_location << " plan_generation=" << m_plan_generation
     << " type=" << m_type << " num_ranges=" << m_plan.receiver_plan.size()
     << " recovery_plan type=" << m_type_str
     << " state=" << OperationState::get_text(get_state());
}

const String OperationRecoverRanges::name() {
  return "OperationRecoverRanges";
}

const String OperationRecoverRanges::label() {
  return format("RecoverServerRanges %s type=%s",
          m_location.c_str(), m_type_str.c_str());
}

void OperationRecoverRanges::initialize_obstructions_dependencies() {
  ScopedLock lock(m_mutex);
  m_dependencies.clear();
  m_obstructions.clear();
  m_obstructions.insert(m_parent_dependency);
  m_dependencies.insert(Dependency::RECOVERY_BLOCKER);
  switch (m_type) {
  case RangeSpec::ROOT:
    m_obstructions.insert(Dependency::ROOT);
    break;
  case RangeSpec::METADATA:
    m_obstructions.insert(Dependency::METADATA);
    m_dependencies.insert(Dependency::ROOT);
    break;
  case RangeSpec::SYSTEM:
    m_obstructions.insert(Dependency::SYSTEM);
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
    break;
  case RangeSpec::USER:
    m_obstructions.insert(format("%s-user", m_location.c_str()));
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
    m_dependencies.insert(Dependency::SYSTEM);
    break;
  }

  vector<QualifiedRangeSpec> specs;
  m_plan.receiver_plan.get_range_specs(specs);
  foreach_ht (QualifiedRangeSpec &spec, specs)
    m_dependencies.insert(format("OperationMove %s[%s..%s]",
                                 spec.table.id, spec.range.start_row,
                                 spec.range.end_row));
}

size_t OperationRecoverRanges::encoded_state_length() const {
  return 2 + Serialization::encoded_length_vstr(m_location) + 
    Serialization::encoded_length_vstr(m_parent_dependency) + 4 + 4 +
    m_plan.encoded_length();
}

void OperationRecoverRanges::encode_state(uint8_t **bufp) const {
  Serialization::encode_i16(bufp, OPERATION_RECOVER_RANGES_VERSION);
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_vstr(bufp, m_parent_dependency);
  Serialization::encode_i32(bufp, m_type);
  Serialization::encode_i32(bufp, m_plan_generation);
  m_plan.encode(bufp);
}

void OperationRecoverRanges::decode_state(const uint8_t **bufp,
        size_t *remainp) {
  decode_request(bufp, remainp);
}

void OperationRecoverRanges::decode_request(const uint8_t **bufp,
        size_t *remainp) {
  // skip version for now
  Serialization::decode_i16(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_parent_dependency = Serialization::decode_vstr(bufp, remainp);
  m_type = Serialization::decode_i32(bufp, remainp);
  m_plan_generation = Serialization::decode_i32(bufp, remainp);
  m_plan.decode(bufp, remainp);
}

bool OperationRecoverRanges::phantom_load_ranges() {
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool success = true;
  StringSet locations;
  m_plan.receiver_plan.get_locations(locations);
  vector<uint32_t> fragments;

  m_plan.replay_plan.get_fragments(fragments);
  foreach_ht (const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> specs;
    vector<RangeState> states;
    m_plan.receiver_plan.get_range_specs_and_states(location, specs, states);
    try {
      HT_INFOF("Calling phantom_load(plan_generation=%d, location=%s) for %d %s ranges",
               m_plan_generation, location.c_str(), (int)specs.size(), m_type_str.c_str());
      rsc.phantom_load(addr, m_location, m_plan_generation, fragments, specs, states);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-phantom-load-ranges",
                  m_type_str.c_str()));
    }
    catch (Exception &e) {
      success = false;
      HT_ERROR_OUT << e << HT_END;
      break;
    }
  }
  if (!success)
    HT_ERROR_OUT << "Failed to issue phantom_load calls" << HT_END;

  return success;
}

bool OperationRecoverRanges::recovery_plan_has_changed() {
  HT_ASSERT(m_plan.type != RangeSpec::UNKNOWN);
  return m_plan_generation !=
    m_context->get_balance_plan_authority()->get_generation();
}

bool OperationRecoverRanges::validate_recovery_plan() {
  HT_ASSERT(m_plan.type != RangeSpec::UNKNOWN);

  return (m_plan_generation
          == m_context->get_balance_plan_authority()->get_generation());
}

bool OperationRecoverRanges::wait_for_quorum() {
  size_t available_servers = m_context->available_server_count();
  size_t total_servers = m_context->rsc_manager->server_count();
  size_t failover_pct =
    m_context->props->get_i32("Hypertable.Failover.Quorum.Percentage");
  size_t quorum = ((total_servers * failover_pct) + 99) / 100;

  if (available_servers < quorum || available_servers == 0) {
    // wait for at least half the servers to be up before proceeding
    // Send notification
    String subject, message;
    subject = format("ERROR: Recovery of %s suspended", m_location.c_str());
    message = format("Recovery of range server %s has been suspended because\\n"
                     "only %d out of %d servers are available.  Required\\n"
                     "quorum is %d.\\n", m_location.c_str(),
                     (int)available_servers, (int)total_servers,
                     (int)quorum);
    m_context->notification_hook(subject, message);
    HT_WARN_OUT << message << HT_END;
    m_context->op->activate(Dependency::RECOVERY_BLOCKER);
    return false;
  }
  return true;
}

void OperationRecoverRanges::create_futures() {
  RecoveryStepFuturePtr future;

  // Install "replay" future
  future = new RecoveryStepFuture("replay", m_plan_generation);
  m_context->recovery_state().install_replay_future(id(), future);

  // Install "prepare" future
  future = new RecoveryStepFuture("prepare", m_plan_generation);
  m_context->recovery_state().install_prepare_future(id(), future);

  // Install "commit" future
  future = new RecoveryStepFuture("commit", m_plan_generation);
  m_context->recovery_state().install_commit_future(id(), future);
}

bool OperationRecoverRanges::get_new_recovery_plan() {
  int initial_generation = m_plan_generation;

  m_context->get_balance_plan_authority()->copy_recovery_plan(m_location,
                                         m_type, m_plan, m_plan_generation);

  if (initial_generation != m_plan_generation) {
    HT_INFOF("Retrieved new balance plan for %s (type=%s, generation=%d) range count %d",
             m_location.c_str(), m_type_str.c_str(), m_plan_generation,
             (int)m_plan.receiver_plan.size());
    create_futures();
    initialize_obstructions_dependencies();
    m_context->mml_writer->record_state(this);
    return true;
  }
  return false;
}

void OperationRecoverRanges::set_type_str() {
  switch (m_type) {
    case RangeSpec::ROOT:
      m_type_str = "root";
      break;
    case RangeSpec::METADATA:
      m_type_str = "metadata";
      break;
    case RangeSpec::SYSTEM:
      m_type_str = "system";
      break;
    case RangeSpec::USER:
      m_type_str = "user";
      break;
    default:
      m_type_str = "UNKNOWN";
  }
}

bool OperationRecoverRanges::replay_fragments() {
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  StringSet locations;
  vector<uint32_t> fragments;

  RecoveryStepFuturePtr future = 
    m_context->recovery_state().get_replay_future(id());

  HT_ASSERT(future);

  m_plan.replay_plan.get_locations(locations);

  future->register_locations(locations);

  foreach_ht(const String &location, locations) {
    try {
      fragments.clear();
      m_plan.replay_plan.get_fragments(location, fragments);
      addr.set_proxy(location);
      HT_INFO_OUT << "Issue replay_fragments for " << fragments.size()
          << " fragments to " << location << " (" << m_type_str << ")"
          << HT_END;
      rsc.replay_fragments(addr, id(), m_location, m_plan_generation, 
                           m_type, fragments, m_plan.receiver_plan, m_timeout);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-replay-fragments",
                           m_type_str.c_str()));
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      future->failure(location, m_plan_generation, e.code(), e.what());
    }
  }

  Timer tt(m_timeout);
  if (!future->wait_for_completion(tt)) {
    String str;
    String message = 
      format("Failure encountered during REPLAY FRAGMENTS step of recovery\\n"
             "of range server %s\\n\\n", m_location.c_str());
    RecoveryStepFuture::ErrorMapT error_map;
    future->get_error_map(error_map);
    for (RecoveryStepFuture::ErrorMapT::iterator iter=error_map.begin();
         iter != error_map.end(); ++iter) {
      str = String("player ") + iter->first + ": " + Error::get_text(iter->second.first)
        + " - " + iter->second.second;
      message += str + "\\n";
      HT_ERROR_OUT << "replay_fragments failed for " << str << HT_END;
    }
    time_t now = time(0);
    if (now - m_last_notification > 60) {
      String subject = format("ERROR: Replay failure during recovery of %s",
                              m_location.c_str());
      m_context->notification_hook(subject, message);
      m_last_notification = now;
    }
    return false;
  }

  return true;
}

bool OperationRecoverRanges::prepare_to_commit() {
  StringSet locations;
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;

  RecoveryStepFuturePtr future = 
    m_context->recovery_state().get_prepare_future(id());

  HT_ASSERT(future);

  m_plan.receiver_plan.get_locations(locations);

  future->register_locations(locations);

  foreach_ht(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> specs;
    m_plan.receiver_plan.get_range_specs(location, specs);

    HT_INFO_OUT << "Issue phantom_prepare_ranges for " << specs.size()
        << " ranges to " << location << " (" << m_type_str << ")" << HT_END;
    try {
      rsc.phantom_prepare_ranges(addr, id(), m_location, m_plan_generation, specs, m_timeout);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-phantom-prepare-ranges",
                  m_type_str.c_str()));
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      future->failure(location, m_plan_generation, e.code(), e.what());
    }
  }

  Timer tt(m_timeout);
  if (!future->wait_for_completion(tt)) {
    HT_ERROR_OUT << "prepare_to_commit failed" << HT_END;
    return false;
  }

  return true;
}

bool OperationRecoverRanges::commit() {
  StringSet locations;
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  BalancePlanAuthority *bpa = m_context->get_balance_plan_authority();

  RecoveryStepFuturePtr future = 
    m_context->recovery_state().get_commit_future(id());

  HT_ASSERT(future);

  if (recovery_plan_has_changed()) {
    StringSet existing_locations, new_locations;
    m_plan.receiver_plan.get_locations(existing_locations);
    bpa->get_receiver_plan_locations(m_location, m_type, new_locations);
    std::set_intersection(existing_locations.begin(), existing_locations.end(),
                          new_locations.begin(), new_locations.end(),
                          std::inserter(locations, locations.end()));
  }
  else
    m_plan.receiver_plan.get_locations(locations);

  // Erase locations marked for "redo"
  foreach_ht (const String &location, m_redo_set)
    locations.erase(location);

  future->register_locations(locations);

  foreach_ht(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> specs;
    m_plan.receiver_plan.get_range_specs(location, specs);

   try {
      HT_INFO_OUT << "Issue phantom_commit_ranges for " << specs.size()
          << " ranges to " << location << HT_END;
      rsc.phantom_commit_ranges(addr, id(), m_location, m_plan_generation, specs, m_timeout);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-phantom-commit-ranges",
                  m_type_str.c_str()));
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      future->failure(location, m_plan_generation, e.code(), e.what());
    }
  }

  Timer tt(m_timeout);
  if (!future->wait_for_completion(tt)) {
    bool retval = true;
    RecoveryStepFuture::ErrorMapT error_map;
    future->get_error_map(error_map);
    for (RecoveryStepFuture::ErrorMapT::iterator iter=error_map.begin();
         iter != error_map.end(); ++iter) {
      if (iter->second.first == Error::RANGESERVER_PHANTOM_RANGE_MAP_NOT_FOUND)
        m_redo_set.insert(iter->first);
      else
        retval = false;
    }
    HT_INFO_OUT << "commit failed" << HT_END;
    return retval;
  }

  return true;
}

bool OperationRecoverRanges::acknowledge() {
  StringSet locations;
  RangeServerClient rsc(m_context->comm);
  CommAddress addr;
  bool success = true;
  vector<QualifiedRangeSpec> acknowledged;
  CharArena arena;
  BalancePlanAuthority *bpa = m_context->get_balance_plan_authority();

  if (recovery_plan_has_changed()) {
    StringSet existing_locations, new_locations;
    m_plan.receiver_plan.get_locations(existing_locations);
    bpa->get_receiver_plan_locations(m_location, m_type, new_locations);
    std::set_intersection(existing_locations.begin(), existing_locations.end(),
                          new_locations.begin(), new_locations.end(),
                          std::inserter(locations, locations.end()));
  }
  else
    m_plan.receiver_plan.get_locations(locations);

  // Erase locations marked for "redo"
  foreach_ht (const String &location, m_redo_set)
    locations.erase(location);

  foreach_ht(const String &location, locations) {
    addr.set_proxy(location);
    vector<QualifiedRangeSpec> specs;
    vector<QualifiedRangeSpec *> range_ptrs;
    map<QualifiedRangeSpec, int> response_map;
    map<QualifiedRangeSpec, int>::iterator response_map_it;

    m_plan.receiver_plan.get_range_specs(location, specs);
    foreach_ht(QualifiedRangeSpec &range, specs)
      range_ptrs.push_back(&range);
    try {
      HT_INFO_OUT << "Issue acknowledge_load for " << range_ptrs.size()
          << " ranges to " << location << HT_END;
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-14", m_type_str.c_str()));
      rsc.acknowledge_load(addr, range_ptrs, response_map);
      HT_MAYBE_FAIL(format("recover-server-ranges-%s-acknowledge-load",
                           m_type_str.c_str()));
      response_map_it = response_map.begin();
      while (response_map_it != response_map.end()) {
        if (response_map_it->second != Error::OK)
          HT_WARNF("Problem acknowledging load for %s[%s..%s] - %s",
                  response_map_it->first.table.id,
                  response_map_it->first.range.start_row,
                  response_map_it->first.range.end_row,
                  Error::get_text(response_map_it->second));
        acknowledged.push_back(QualifiedRangeSpec(arena, response_map_it->first));
        ++response_map_it;
      }
      HT_INFO_OUT << "acknowledge_load complete for " << range_ptrs.size()
          << " ranges to " << location << HT_END;
    }
    catch (Exception &e) {
      success = false;
      HT_ERROR_OUT << e << HT_END;
    }
  }

  // Purge successfully acknowledged ranges from recovery plan(s)
  if (!acknowledged.empty()) {
    bpa->remove_from_receiver_plan(m_location, m_type, acknowledged);
    foreach_ht (const QualifiedRangeSpec &spec, acknowledged)
      m_plan.receiver_plan.remove(spec);
  }

  // at this point all the players have prepared or failed in
  // creating phantom ranges
  return success;
}


/*
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

#include <Common/Compat.h>

#include "OperationMoveRange.h"
#include "OperationProcessor.h"
#include "OperationRelinquishAcknowledge.h"
#include "ReferenceManager.h"
#include "Utility.h"

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>
#include <Common/StringExt.h>
#include <Common/System.h>
#include <Common/md5.h>

using namespace Hypertable;

OperationRelinquishAcknowledge::
OperationRelinquishAcknowledge(ContextPtr &ctx, const String &source,
                               int64_t range_id, TableIdentifier &table,
                               RangeSpec &range)
  : OperationEphemeral(ctx, MetaLog::EntityType::OPERATION_RELINQUISH_ACKNOWLEDGE),
    m_params(source, range_id, table, range) {
}

OperationRelinquishAcknowledge::OperationRelinquishAcknowledge(ContextPtr &context, EventPtr &event) 
  : OperationEphemeral(context, event,
                       MetaLog::EntityType::OPERATION_RELINQUISH_ACKNOWLEDGE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  m_dependencies.insert(Dependency::INIT);
}

void OperationRelinquishAcknowledge::execute() {

  HT_INFOF("Entering RelinquishAcknowledge-%lld %s[%s..%s] (id=%lld) "
           "source=%s state=%s", (Lld)header.id, m_params.table().id,
           m_params.range_spec().start_row, m_params.range_spec().end_row,
           (Lld)m_params.range_id(), m_params.source().c_str(),
           OperationState::get_text(m_state));

  HT_MAYBE_FAIL("relinquish-acknowledge-INITIAL-a");
  HT_MAYBE_FAIL("relinquish-acknowledge-INITIAL-b");

  int64_t hash_code =
    OperationMoveRange::hash_code(m_params.table(), m_params.range_spec(),
                                  m_params.source(), m_params.range_id());

  OperationPtr operation = m_context->get_move_operation(hash_code);
  if (operation) {
    if (operation->remove_approval_add(0x01)) {
      operation->record_state();
      m_context->remove_move_operation(operation);
    }
  }
  else
    HT_WARNF("Skipping relinquish_acknowledge(%s %s[%s..%s] "
             "because correspoing MoveRange does not exist",
             m_params.source().c_str(), m_params.table().id,
             m_params.range_spec().start_row, m_params.range_spec().end_row);

  complete_ok();

  HT_INFOF("Leaving RelinquishAcknowledge-%lld %s[%s..%s] (id=%lld) "
           "from %s (state=%s)", (Lld)header.id, m_params.table().id,
           m_params.range_spec().start_row, m_params.range_spec().end_row,
           (Lld)m_params.range_id(), m_params.source().c_str(),
           OperationState::get_text(m_state));
}

void OperationRelinquishAcknowledge::display_state(std::ostream &os) {
  os << " " << m_params.table() << " " << m_params.range_spec()
     << " source=" << m_params.source();
}

const String OperationRelinquishAcknowledge::name() {
  return "OperationRelinquishAcknowledge";
}

const String OperationRelinquishAcknowledge::label() {
  return format("RelinquishAcknowledge %s[%s..%s]", m_params.table().id,
                m_params.range_spec().start_row, m_params.range_spec().end_row);
}

const String OperationRelinquishAcknowledge::graphviz_label() {
  String start_row = m_params.range_spec().start_row;
  String end_row = m_params.range_spec().end_row;

  if (start_row.length() > 20)
    start_row = start_row.substr(0, 10) + ".." +
      start_row.substr(start_row.length()-10, 10);

  if (end_row.length() > 20)
    end_row = end_row.substr(0, 10) + ".." +
      end_row.substr(end_row.length()-10, 10);
  
  return format("RelinquishAcknowledge %s\\n%s\\n%s",
                m_params.table().id, start_row.c_str(), end_row.c_str());
}

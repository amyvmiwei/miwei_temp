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

#include "MetaLogDefinitionMaster.h"

#include "OperationAlterTable.h"
#include "OperationBalance.h"
#include "OperationCompact.h"
#include "OperationCreateNamespace.h"
#include "OperationCreateTable.h"
#include "OperationDropTable.h"
#include "OperationDropNamespace.h"
#include "OperationInitialize.h"
#include "OperationMoveRange.h"
#include "OperationRenameTable.h"
#include "OperationRecover.h"
#include "OperationRecoverRanges.h"
#include "OperationRecoveryBlocker.h"
#include "OperationRecreateIndexTables.h"
#include "OperationSetState.h"
#include "OperationToggleTableMaintenance.h"
#include "RangeServerConnection.h"
#include "BalancePlanAuthority.h"
#include "SystemState.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

uint16_t DefinitionMaster::version() {
  return 2;
}

const char *DefinitionMaster::name() {
  return "mml";
}

Entity *DefinitionMaster::create(const EntityHeader &header) {
  Operation *operation = 0;

  if (header.type == EntityType::RANGE_SERVER_CONNECTION)
    return new RangeServerConnection(header);

  if ((header.type & 0xF0000L) == 0x20000L) {

    /*
     * If old operation, then record the original type and convert
     */

    if (header.type == EntityType::OLD_OPERATION_INITIALIZE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_INITIALIZE;
      operation = new OperationInitialize(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_INITIALIZE);
    }
    else if (header.type == EntityType::OLD_OPERATION_ALTER_TABLE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_ALTER_TABLE;
      operation = new OperationAlterTable(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_ALTER_TABLE);
    }
    else if (header.type == EntityType::OLD_OPERATION_CREATE_NAMESPACE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_CREATE_NAMESPACE;
      operation = new OperationCreateNamespace(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_CREATE_NAMESPACE);
    }
    else if (header.type == EntityType::OLD_OPERATION_DROP_NAMESPACE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_DROP_NAMESPACE;
      operation = new OperationDropNamespace(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_DROP_NAMESPACE);
    }
    else if (header.type == EntityType::OLD_OPERATION_CREATE_TABLE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_CREATE_TABLE;
      operation = new OperationCreateTable(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_CREATE_TABLE);
    }
    else if (header.type == EntityType::OLD_OPERATION_DROP_TABLE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_DROP_TABLE;
      operation = new OperationDropTable(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_DROP_TABLE);
    }
    else if (header.type == EntityType::OLD_OPERATION_RENAME_TABLE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_RENAME_TABLE;
      operation = new OperationRenameTable(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_RENAME_TABLE);
    }
    else if (header.type == EntityType::OLD_OPERATION_MOVE_RANGE) {
      ((EntityHeader *)&header)->type = EntityType::OPERATION_MOVE_RANGE;
      operation = new OperationMoveRange(m_context, header);
      operation->set_original_type(EntityType::OLD_OPERATION_MOVE_RANGE);
    }
    else if (header.type == EntityType::OLD_OPERATION_BALANCE) {
      return 0;
    }
  }
  else if (header.type == EntityType::BALANCE_PLAN_AUTHORITY) {
    MetaLog::WriterPtr mml_writer = m_context ? m_context->mml_writer : 0;
    return new BalancePlanAuthority(m_context.get(), mml_writer, header);
  }
  else {
    if (header.type == EntityType::OPERATION_INITIALIZE)
      operation = new OperationInitialize(m_context, header);
    else if (header.type == EntityType::OPERATION_ALTER_TABLE)
      operation = new OperationAlterTable(m_context, header);
    else if (header.type == EntityType::OPERATION_CREATE_NAMESPACE)
      operation = new OperationCreateNamespace(m_context, header);
    else if (header.type == EntityType::OPERATION_DROP_NAMESPACE)
      operation = new OperationDropNamespace(m_context, header);
    else if (header.type == EntityType::OPERATION_CREATE_TABLE)
      operation = new OperationCreateTable(m_context, header);
    else if (header.type == EntityType::OPERATION_DROP_TABLE)
      operation = new OperationDropTable(m_context, header);
    else if (header.type == EntityType::OPERATION_RENAME_TABLE)
      operation = new OperationRenameTable(m_context, header);
    else if (header.type == EntityType::OPERATION_MOVE_RANGE)
      operation = new OperationMoveRange(m_context, header);
    else if (header.type == EntityType::OPERATION_BALANCE_RETIRED)
      return 0;
    else if (header.type == EntityType::OPERATION_RECOVER_SERVER)
      operation = new OperationRecover(m_context, header);
    else if (header.type == EntityType::OPERATION_RECOVER_SERVER_RANGES)
      operation = new OperationRecoverRanges(m_context, header);
    else if (header.type == EntityType::OPERATION_BALANCE)
      operation = new OperationBalance(m_context, header);
    else if (header.type == EntityType::OPERATION_COMPACT)
      operation = new OperationCompact(m_context, header);
    else if (header.type == EntityType::OPERATION_SET)
      operation = new OperationSetState(m_context, header);
    else if (header.type == EntityType::OPERATION_TOGGLE_TABLE_MAINTENANCE)
      operation = new OperationToggleTableMaintenance(m_context, header);
    else if (header.type == EntityType::OPERATION_RECREATE_INDEX_TABLES)
      operation = new OperationRecreateIndexTables(m_context, header);
    else if (header.type == EntityType::SYSTEM_STATE)
      return new SystemState(header);
  }

  if (operation)
    return operation;

  HT_THROWF(Error::METALOG_ENTRY_BAD_TYPE,
            "Unrecognized type (%d) encountered in mml",
            (int)header.type);
}


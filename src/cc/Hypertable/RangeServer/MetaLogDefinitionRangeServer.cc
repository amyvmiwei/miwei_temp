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

#include <Common/Compat.h>

#include "MetaLogDefinitionRangeServer.h"

#include "MetaLogEntityRange.h"
#include "MetaLogEntityRemoveOkLogs.h"
#include "MetaLogEntityTaskAcknowledgeRelinquish.h"

#include <memory>

using namespace Hypertable;
using namespace Hypertable::MetaLog;
using namespace std;

uint16_t DefinitionRangeServer::version() {
  return 3;
}

const char *DefinitionRangeServer::name() {
  return "rsml";
}

EntityPtr DefinitionRangeServer::create(const EntityHeader &header) {

  if (header.type == EntityType::RANGE)
    return make_shared<MetaLogEntityRange>(header);
  else if (header.type == EntityType::RANGE2)
    return make_shared<MetaLogEntityRange>(header);
  else if (header.type == EntityType::TASK_REMOVE_TRANSFER_LOG)
    return EntityPtr();  // no longer used
  else if (header.type == EntityType::TASK_ACKNOWLEDGE_RELINQUISH)
    return make_shared<EntityTaskAcknowledgeRelinquish>(header);
  else if (header.type == EntityType::REMOVE_OK_LOGS)
    return make_shared<MetaLogEntityRemoveOkLogs>(header);

  HT_THROWF(Error::METALOG_ENTRY_BAD_TYPE,
            "Unrecognized type (%d) encountered in rsml",
            (int)header.type);

}


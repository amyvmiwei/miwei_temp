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

#ifndef HYPERTABLE_METALOGDEFINITIONMASTER_H
#define HYPERTABLE_METALOGDEFINITIONMASTER_H

#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/MetaLogDefinition.h"

#include "Context.h"

namespace Hypertable {
  namespace MetaLog {
    class DefinitionMaster : public Definition {
    public:
      DefinitionMaster(const char *backup_label) : Definition(backup_label) { }
      DefinitionMaster(ContextPtr &context, const char *backup_label) : Definition(backup_label)
          , m_context(context) { }
      virtual uint16_t version();
      virtual bool supported_version(uint16_t ver);
      virtual const char *name();
      virtual Entity *create(uint16_t log_version, const EntityHeader &header);
    private:
      ContextPtr m_context;
    };
    typedef intrusive_ptr<DefinitionMaster> DefinitionMasterPtr;
  }

}

#endif // HYPERTABLE_METALOGDEFINITIONMASTER_H

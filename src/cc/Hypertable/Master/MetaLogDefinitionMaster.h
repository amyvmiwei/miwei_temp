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

#ifndef Hypertable_Master_MetaLogDefinitionmaster_h
#define Hypertable_Master_MetaLogDefinitionmaster_h

#include "Context.h"

#include <Hypertable/Lib/MetaLogDefinition.h>

namespace Hypertable {
namespace MetaLog {

  class DefinitionMaster : public Definition {
  public:
    DefinitionMaster(const char *backup_label) : Definition(backup_label) { }
    DefinitionMaster(ContextPtr &context, const char *backup_label) : Definition(backup_label)
                                                                    , m_context(context) { }
    uint16_t version() override;
    const char *name() override;
    EntityPtr create(const EntityHeader &header) override;
  private:
    ContextPtr m_context;
  };

}}

#endif // Hypertable_Master_MetaLogDefinitionmaster_h

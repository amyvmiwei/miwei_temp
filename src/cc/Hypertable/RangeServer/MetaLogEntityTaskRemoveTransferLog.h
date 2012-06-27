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

#ifndef HYPERTABLE_METALOGENTITYTASKREMOVETRANSFERLOG_H
#define HYPERTABLE_METALOGENTITYTASKREMOVETRANSFERLOG_H

#include "MetaLogEntityTask.h"

namespace Hypertable {
  namespace MetaLog {

    class EntityTaskRemoveTransferLog : public EntityTask {
    public:
      EntityTaskRemoveTransferLog(const EntityHeader &header_);
      EntityTaskRemoveTransferLog(const String &transfer_log);
      virtual ~EntityTaskRemoveTransferLog() { }
      virtual bool execute();
      virtual size_t encoded_length() const;
      virtual void encode(uint8_t **bufp) const;
      virtual void decode(const uint8_t **bufp, size_t *remainp);
      virtual const String name();
      virtual void display(std::ostream &os);

      String transfer_log;
    };

    namespace EntityType {
      enum {
        TASK_REMOVE_TRANSFER_LOG = 0x00010003
      };
    }


  } // namespace MetaLog
} // namespace Hypertable

#endif // HYPERTABLE_METALOGENTITYTASKREMOVETRANSFERLOG_H

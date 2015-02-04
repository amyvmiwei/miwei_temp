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

#ifndef HYPERTABLE_METALOGENTITYTASKACKNOWLEDGERELINQUISH_H
#define HYPERTABLE_METALOGENTITYTASKACKNOWLEDGERELINQUISH_H

#include "Common/md5.h"

#include "MetaLogEntityTask.h"

namespace Hypertable {
  namespace MetaLog {

    class EntityTaskAcknowledgeRelinquish : public EntityTask {
    public:
      EntityTaskAcknowledgeRelinquish(const EntityHeader &header_);
      EntityTaskAcknowledgeRelinquish(const String &loc, int64_t id,
                                      const TableIdentifier &t,
                                      const RangeSpec &rs);
      virtual ~EntityTaskAcknowledgeRelinquish() { }
      virtual bool execute();
      virtual void work_queue_add_hook();

      /** Decodes serlialized EntityTaskAcknowledgeRelinquish object.
       * @param bufp Address of source buffer pointer (advanced by call)
       * @param remainp Amount of remaining buffer pointed to by
       * <code>*bufp</code> (decremented by call).
       * @param definition_version Version of DefinitionMaster
       */
      void decode(const uint8_t **bufp, size_t *remainp,
                  uint16_t definition_version) override;
      virtual const String name();
      virtual void display(std::ostream &os);

      String location;
      int64_t range_id {};
      TableIdentifierManaged table;
      RangeSpecManaged range_spec;

    private:

      uint8_t encoding_version() const override;

      size_t encoded_length_internal() const override;

      void encode_internal(uint8_t **bufp) const override;

      void decode_internal(uint8_t version, const uint8_t **bufp,
                           size_t *remainp) override;

      void decode_old(const uint8_t **bufp, size_t *remainp);
      
    };

  } // namespace MetaLog
} // namespace Hypertable

#endif // HYPERTABLE_METALOGENTITYTASKACKNOWLEDGERELINQUISH_H

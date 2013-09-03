/* -*- c++ -*-
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

/** @file
 * Declarations for MetaLog::Entity.
 * This file contains declarations for MetaLog::Entity, an abstract base class
 * for persisting an object's state in a %MetaLog
 */

#ifndef HYPERTABLE_METALOGENTITY_H
#define HYPERTABLE_METALOGENTITY_H

#include <iostream>

extern "C" {
#include <time.h>
}

#include <boost/algorithm/string.hpp>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "MetaLogEntityHeader.h"

namespace Hypertable {

  namespace MetaLog {

    /** @addtogroup libHypertable
     * @{
     */

    class Reader;
    class Writer;

    /** Base class for %MetaLog entities.
     * A %MetaLog entitiy is associated with each application object that is
     * to be persisted in the %MetaLog.  Entity objects are what get passed
     * into and out of the read and write methods of the MetaLog API.
     * Application objects can include a member that is derived from this
     * class for the purpose of persisting its state.  The application object
     * class can also be derived from Entity allowing the application object
     * itself to be passed directly into the %Metalog APIs.
     */
    class Entity : public ReferenceCount {
    public:

      /** Constructor from entity type.
       * This method constructs a new entity, initializing the #header member
       * with <code>type</code>.
       * @param type Numeric entity type
       */
      Entity(int32_t type);

      /** Constructor from entity header.
       * This method constructs an entity from an entity header (typically read
       * and deserialized from a %MetaLog).  The #header member is initialized
       * with <code>header_</code>.
       * @param header_ %Entity header 
       */
      Entity(const EntityHeader &header_);

      /** Destructor. */
      virtual ~Entity() { }

      /** Returns length of serialized entity state (excluding header).
       * @return Length of serialized entity state.
       */
      virtual size_t encoded_length() const = 0;

      /** Writes serialized entity state (excluding header) to a buffer.
       * @param bufp Address of destination buffer pointer (advanced by call)
       */
      virtual void encode(uint8_t **bufp) const = 0;

      /** Decodes serialized entity state.
       * @param bufp Address of destination buffer pointer (advanced by call)
       * @param remainp Address of integer holding amount of remaining buffer
       * @param definition_version Version of MetaLog::Definition that was used
       * to generate the log.
       */
      virtual void decode(const uint8_t **bufp, size_t *remainp,
                          uint16_t definition_version) = 0;

      /** Locks the entity's mutex. */
      void lock() { m_mutex.lock(); }

      /** Unlocks the entity's mutex. */
      void unlock() { m_mutex.unlock(); }

      /** Marks entity for removal.
       * To remove an entity from a %MetaLog, the entity header gets written to
       * the log with the EntityHeader::FLAG_REMOVE bit set in the flags field
       * of the header.  This method sets the EntityHeader::FLAG_REMOVE bit in
       * the header and sets the header length and header checksum fields to
       * zero.  After a call to this method, the next time the entity is
       * persisted to the log, it will be logically removed.
       */
      void mark_for_removal() {
        header.flags |= EntityHeader::FLAG_REMOVE;
        header.length = header.checksum = 0;
      }

      /** Checks if entity is marked for removal.
       * This method returns <i>true</i> if the EntityHeader::FLAG_REMOVE bit is
       * set in the <i>flags</i> member of its header which means the next time
       * it gets persisted, it will be logically removed removed.
       * @see mark_for_removal()
       * @return <i>true</i> if entity is marked for removal, <i>false</i>
       * otherwise 
       */
      bool marked_for_removal() {
        return (header.flags & EntityHeader::FLAG_REMOVE) != 0;
      }

      /** Returns the name of the entity.
       * This method returns the name of the entity which is used by tools that
       * convert a %MetaLog into a human readable representation
       * @return Name of the entity
       */
      virtual const String name() = 0;

      /** Returns the entity ID.
       * @return Entity ID from the 
       */
      int64_t id() const { return header.id; }

      /** Prints a textual representation of the entity state to an ostream.
       * @param os ostream on which to print entity state
       */
      virtual void display(std::ostream &os) { }

      friend std::ostream &operator <<(std::ostream &os, Entity &entity);
      friend class Reader;
      friend class Writer;

    protected:

      /** Encodes entity header plus serialized state.
       * This method first encodes the serialized state at <code>*bufp</code>
       * plus EntityHeader::LENGTH bytes.  It then computes the checksum of
       * the serialized state with a call to fletcher32() and stores the
       * result in <code>header.checksum</code>.  It then encodes #header
       * at the beginning of the buffer.  <code>*bufp</code> is left pointing
       * to the end of the serialized state when the method returns.
       * @param bufp Address of destination buffer pointer (advanced by call)
       */
      void encode_entry(uint8_t **bufp);

      /// %Mutex for serializing access to members
      Mutex m_mutex;

      /// %Entity header
      EntityHeader header;
    };

    /// Smart pointer to Entity
    typedef intrusive_ptr<Entity> EntityPtr;

    /** ostream shift function for Entity objects.
     * This method writes a human readable representation of an Entity
     * to a given ostream.  It prints the header followed by the entity state.
     * @param os ostream on which to print entity
     * @param entity Entity object to print
     * @return <code>os</code>
     */
    inline std::ostream &
    operator <<(std::ostream &os, Entity &entity) {
      os << "{MetaLog::Entity " << entity.name() << " header={";
      entity.header.display(os);
      os << "} payload={";
      entity.display(os);
      os << "}";
      return os;
    }

    namespace EntityType {
      enum {
        RECOVER = 0x00000001 ///< Recover entity
      };
    }

    /** Recover entity used for sanity checking.
     * When a %MetaLog is opened for writing, a set of entities are provided
     * which first get written to the new log (see Writer()).  The log file is
     * kept open by the MetaLog::Writer during the execution of the server and
     * application object state changes are recorded in the log by updating the
     * state of the associated entity object and appending the updated entity
     * to the log file.  To detect the situation when a server exits before the
     * initially provided entities were completely written to the log, an
     * EntityRecover object is written to the log after all of the initially
     * provided entities.  When the MetaLog::Reader reads a %MetaLog file,
     * it checks for this entity and if not found, it throws and Exception
     * with error code Error::METALOG_MISSING_RECOVER_ENTITY.
     */
    class EntityRecover : public Entity {
    public:

      /** Constructor.
       * Initializes parent Entity constructor with EntityType::RECOVER.
       */
      EntityRecover() : Entity(EntityType::RECOVER) { }

      /** Returns length of serialized entity state 
       * Since the EntityRecover is a meta entity used internally to
       * signal the end of the initially supplied entities, it contains
       * no state and therefore this method returns 0.
       * @return 0
       */
      virtual size_t encoded_length() const { return 0; }

      /** Writes serialized entity state to a buffer.
       * Since the EntityRecover is a meta entity used internally to
       * signal the end of the initially supplied entities, it contains
       * no state and therefore this method is a no-op.
       * @param bufp Address of destination buffer pointer
       */
      virtual void encode(uint8_t **bufp) const { }

      /** Decodes serialized entity state.
       * Since the EntityRecover is a meta entity used internally to
       * signal the end of the initially supplied entities, it contains
       * no state and therefore this method is a no-op.
       * @param bufp Address of destination buffer pointer
       * @param remainp Address of integer holding amount of remaining buffer
       * @param definition_version Version of MetaLog::Definition that was used
       * to generate the log.
       */
      virtual void decode(const uint8_t **bufp, size_t *remainp,
                          uint16_t definition_version) { }

      /** Returns the name of the entity.
       * This method returns the name of the entity ("Recover")
       * @return Name of the entity ("Recover")
       */
      virtual const String name() { return "Recover"; }
    };
    /** @}*/
  }
}

#endif // HYPERTABLE_METALOGENTITY_H

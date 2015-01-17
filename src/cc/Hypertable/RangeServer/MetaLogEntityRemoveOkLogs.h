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

/** @file
 * Declarations for MetaLogEntityRemoveOkLogs.
 * This file contains the type declarations for MetaLogEntityRemoveOkLogs,
 * a %MetaLog entity class used to track transfer logs that can be
 * safely removed.
 */

#ifndef Hypertable_RangeServer_MetaLogEntityRemoveOkLogs_h
#define Hypertable_RangeServer_MetaLogEntityRemoveOkLogs_h

#include "MetaLogEntityTypes.h"

#include <Hypertable/Lib/MetaLogEntity.h>
#include <Hypertable/Lib/RangeState.h>

#include <Common/StringExt.h>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /** %MetaLog entity to track transfer logs that can be safely removed
   */
  class MetaLogEntityRemoveOkLogs : public MetaLog::Entity {
  public:

    /** Constructor initialized from %Metalog entity header.
     * @param header_ %Metalog entity header
     */
    MetaLogEntityRemoveOkLogs(const MetaLog::EntityHeader &header_);

    /** Constructor.
     * @param logs Initial set of log pathnames
     */
    MetaLogEntityRemoveOkLogs(StringSet &logs);

    /** Constructor. */
    MetaLogEntityRemoveOkLogs();

    /** Destructor */
    virtual ~MetaLogEntityRemoveOkLogs() { }

    /** Inserts a log into the remove ok set
     * @param pathname Pathname of log to insert
     */
    void insert(const String &pathname);

    /** Inserts a set of logs into the remove ok set
     * @param logs Set of logs to insert
     */
    void insert(StringSet &logs);

    /** Removes logs from the remove ok set
     * @param logs Logs to remove
     */
    void remove(StringSet &logs);

    /** Gets logs in the remove ok set
     * @param logs Output parameter filled in with logs
     */
    void get(StringSet &logs);

    /** Returns format of decoded entity.
     * @return Format of decoded entity.
     */
    uint32_t decode_version() {
      return m_decode_version;
    }

    /** Reads serialized encoding of the entity.
     * This method restores the state of the object by decoding a serialized
     * representation of the state from the memory location pointed to by
     * <code>*bufp</code>.
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by
     * <code>*bufp</code> (decremented by call).
     * @param definition_version Version of DefinitionMaster
     * @see encode() for serialization format
     */    
    void decode(const uint8_t **bufp, size_t *remainp,
                uint16_t definition_version) override;

    /** Returns the entity name ("Range")
     * @return %Entity name
     */
    virtual const String name();

    /** Writes a human readable representation of the object state to
     * an output stream.
     * @param os Output stream
     */
    virtual void display(std::ostream &os);

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
                         size_t *remainp) override;

    void decode_old(const uint8_t **bufp, size_t *remainp);

    /// Set of log pathnames that can be safely removed
    StringSet m_log_set;

    /// Version of serialized entity decoded
    uint32_t m_decode_version;

  };

  /// Smart pointer to MetaLogEntityRemoveOkLogs
  typedef std::shared_ptr<MetaLogEntityRemoveOkLogs> MetaLogEntityRemoveOkLogsPtr;

  /// @}

}

#endif // Hypertable_RangeServer_MetaLogEntityRemoveOkLogs_h

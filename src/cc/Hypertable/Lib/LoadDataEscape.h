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

#ifndef HYPERTABLE_LOADDATAESCAPE_H
#define HYPERTABLE_LOADDATAESCAPE_H

#include "Common/DynamicBuffer.h"

namespace Hypertable {

  class LoadDataEscape {

  public:
    bool escape(const char *in_buf, size_t in_len, const char **out_bufp,
                size_t *out_lenp);
    bool unescape(const char *in_buf, size_t in_len, const char **out_bufp,
                  size_t *out_lenp);

  private:
    DynamicBuffer m_buf;

  };

} // namespace Hypertable

#endif // HYPERTABLE_LOADDATAESCAPE_H

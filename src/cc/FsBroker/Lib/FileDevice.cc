/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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

#include "FileDevice.h"
#include "Common/StaticBuffer.h"
#include "Common/Error.h"

using namespace Hypertable::FsBroker;
using namespace std;
using namespace boost::iostreams;




FileDevice::FileDevice(ClientPtr &client, const String &filename,
        bool accurate_length, BOOST_IOS::openmode mode)
{
  open(client, filename, accurate_length, mode);
}

void FileDevice::open(ClientPtr &client, const String &filename,
        bool accurate_length, BOOST_IOS::openmode mode)
{
  pimpl_.reset(new impl(client, filename, accurate_length, mode));
}

bool FileDevice::is_open() const
{
  return pimpl_->m_open;
}

size_t FileDevice::read(char_type *dst, size_t amount)
{
  return pimpl_->read(dst, amount);
}

size_t FileDevice::bytes_read()
{
  return pimpl_->bytes_read();
}

size_t FileDevice::length()
{
  return pimpl_->length();
}

size_t FileDevice::write(const char_type *dst, size_t amount)
{
  return pimpl_->write(dst, amount);
}

size_t FileDevice::bytes_written()
{
  return pimpl_->bytes_written();
}
void FileDevice::close()
{
  pimpl_->close();
}

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

#include "Common/Compat.h"
#include "Common/Init.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "Common/StatsSystem.h"
#include "Common/String.h"

#include <vector>

using namespace Hypertable;


int main(int argc, char *argv[]) {
  Config::init(argc, argv);
  std::vector<String> dirs;

  dirs.push_back("/");
  dirs.push_back("/tmp");

  StatsSystem *stats = new StatsSystem(StatsSystem::CPUINFO |
                                       StatsSystem::CPU |
                                       StatsSystem::LOADAVG |
                                       StatsSystem::MEMORY |
                                       StatsSystem::DISK |
                                       StatsSystem::SWAP |
                                       StatsSystem::NETINFO |
                                       StatsSystem::NET |
                                       StatsSystem::OSINFO |
                                       StatsSystem::PROCINFO |
                                       StatsSystem::PROC |
                                       StatsSystem::FS |
                                       StatsSystem::TERMINFO,
                                       dirs);

  size_t len = stats->encoded_length();
  
  uint8_t *buf = new uint8_t[ len ];
  uint8_t *ptr = buf;

  stats->encode(&ptr);

  HT_ASSERT((size_t)(ptr-buf) == len);

  StatsSystem *stats2 = new StatsSystem();

  const uint8_t *ptr2 = buf;
  stats2->decode(&ptr2, &len);

  HT_ASSERT(len == 0);

  HT_ASSERT(*stats == *stats2);

}

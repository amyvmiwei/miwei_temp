/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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

/// @file
/// Ddefinitions for GangliaMetrics.
/// This file contains type ddefinitions for GangliaMetrics, a simple class for
/// aggregating metrics and sending them to the Ganglia gmond process running on
/// localhost.

#include <Common/Compat.h>

#include "GangliaMetrics.h"

#include <Common/Logger.h>

extern "C" {
#if defined(__APPLE__) || defined(__sun__) || defined(__FreeBSD__)
#include <arpa/inet.h>
#include <netinet/ip.h>
#endif
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
}

#include <cerrno>

using namespace Hypertable;
using namespace std;

GangliaMetrics::GangliaMetrics(uint16_t port) : m_port(port) {
  InetAddr local_addr(INADDR_ANY, 0);

  if ((m_sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    HT_FATALF("socket(AF_INET, SOCK_DGRAM, 0) failure - %s", strerror(errno));

  {
    int opt;
#if defined(__linux__)
    opt = 0x10;
    setsockopt(m_sd, SOL_IP, IP_TOS, &opt, sizeof(opt));
    opt = 0x10;
    setsockopt(m_sd, SOL_SOCKET, SO_PRIORITY, &opt, sizeof(opt));
#elif defined(__APPLE__) || defined(__sun__) || defined(__FreeBSD__)
    opt = IPTOS_LOWDELAY;       /* see <netinet/in.h> */
    setsockopt(m_sd, IPPROTO_IP, IP_TOS, &opt, sizeof(opt));
#endif
  }

  if ((::bind(m_sd, (const sockaddr *)&local_addr, sizeof(sockaddr_in))) < 0)
    HT_FATALF("bind(%s) failure - %s", local_addr.format().c_str(), strerror(errno));

}

GangliaMetrics::~GangliaMetrics() {
  ::close(m_sd);
}

void GangliaMetrics::update(const std::string &name, const std::string &value) {
  m_values_string[name] = value;
}

void GangliaMetrics::update(const std::string &name, int16_t value) {
  m_values_int[name] = (int32_t)value;
}

void GangliaMetrics::update(const std::string &name, int32_t value) {
  m_values_int[name] = value;
}

void GangliaMetrics::update(const std::string &name, float value) {
  m_values_double[name] = (double)value;
}

void GangliaMetrics::update(const std::string &name, double value) {
  m_values_double[name] = value;
}

bool GangliaMetrics::send() {
  if (!m_connected) {
    if (!this->connect())
      return false;
  }

  bool first = true;
  char cbuf[64];

  m_message.clear();
  m_message.append("{ ");

  for (auto & entry : m_values_string) {
    if (!first)
      m_message.append(", \"");
    else {
      m_message.append("\"");
      first = false;
    }
    m_message.append(entry.first);
    m_message.append("\": \"");
    m_message.append(entry.second);
    m_message.append("\"");
  }

  for (auto & entry : m_values_int) {
    if (!first)
      m_message.append(", \"");
    else {
      m_message.append("\"");
      first = false;
    }
    m_message.append(entry.first);
    m_message.append("\": ");
    sprintf(cbuf, "%d", entry.second);
    m_message.append(cbuf);
  }

  for (auto & entry : m_values_double) {
    if (!first)
      m_message.append(", \"");
    else {
      m_message.append("\"");
      first = false;
    }
    m_message.append(entry.first);
    m_message.append("\": ");
    sprintf(cbuf, "%f", entry.second);
    m_message.append(cbuf);
  }

  m_message.append(" }");

  if (::send(m_sd, m_message.c_str(), m_message.length(), 0) < 0) {
    m_error = strerror(errno);
    return false;
  }

  m_error.clear();
  return true;
}

bool GangliaMetrics::connect() {
  InetAddr addr("localhost", m_port);
  if (::connect(m_sd, (struct sockaddr *) &addr, sizeof(sockaddr_in)) < 0) {
    m_error = strerror(errno);
    m_connected = false;
  }
  else {
    m_error.clear();
    m_connected = true;
  }
  return m_connected;
}

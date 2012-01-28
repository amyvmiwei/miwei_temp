<?php
#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

//$GLOBALS['THRIFT_ROOT'] = '/Users/luke/Source/thrift/lib/php/src';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';

/**
 * Suppress errors in here, which happen because we have not installed into
 * $GLOBALS['THRIFT_ROOT'].'/packages/tutorial' like they assume!
 *
 * Normally we would only have to include HqlService.php which would properly
 * include the other files from their packages/ folder locations, but we
 * include everything here due to the bogus path setup.
 */
$old_error_reporting = error_reporting();
error_reporting(0); // E_NONE
$GEN_DIR = dirname(__FILE__).'/gen-php';
require_once $GEN_DIR.'/Client/ClientService.php';
require_once $GEN_DIR.'/Client/Client_types.php';
require_once $GEN_DIR.'/Hql/HqlService.php';
require_once $GEN_DIR.'/Hql/Hql_types.php';
error_reporting($old_error_reporting);

class Hypertable_ThriftClient extends Hypertable_ThriftGen_HqlServiceClient {
  function __construct($host, $port, $timeout_ms = 300000, $do_open = true) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($timeout_ms);
    $socket->setRecvTimeout($timeout_ms);
    $this->transport = new TFramedTransport($socket);
    $protocol = new TBinaryProtocol($this->transport);
    parent::__construct($protocol);

    if ($do_open)
      $this->open();
  }

  function __destruct() { $this->close(); }

  function open() {
    $this->transport->open();
    $this->do_close = true;
  }

  function close() {
    if ($this->do_close)
      $this->transport->close();
  }
}

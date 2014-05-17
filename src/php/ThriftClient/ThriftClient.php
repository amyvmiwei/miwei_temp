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
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/Exception/TException.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/Factory/TStringFuncFactory.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/Protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/StringFunc/TStringFunc.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/StringFunc/Core.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/Transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/Transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/Type/TMessageType.php';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift/Type/TType.php';

/**
 * Suppress errors in here, which happen because we have not installed into
 * $GLOBALS['THRIFT_ROOT'].'/packages/tutorial' like they assume!
 *
 * Normally we would only have to include HqlService.php which would properly
 * include the other files from their packages/ folder locations, but we
 * include everything here due to the bogus path setup.
 */
require_once $GLOBALS['THRIFT_ROOT'].'/gen-php/Hypertable_ThriftGen/ClientService.php';
require_once $GLOBALS['THRIFT_ROOT'].'/gen-php/Hypertable_ThriftGen/Types.php';
require_once $GLOBALS['THRIFT_ROOT'].'/gen-php/Hypertable_ThriftGen2/HqlService.php';
require_once $GLOBALS['THRIFT_ROOT'].'/gen-php/Hypertable_ThriftGen2/Types.php';

use Thrift\Transport\TSocket;
use Thrift\Transport\TFramedTransport;
use Thrift\Protocol\TBinaryProtocol;

class Hypertable_ThriftClient extends \Hypertable_ThriftGen2\HqlServiceClient {
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

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

if (!isset($GLOBALS['THRIFT_ROOT']))
    $GLOBALS['THRIFT_ROOT'] = getenv('PHPTHRIFT_ROOT');

require_once dirname(__FILE__).'/ThriftClient.php';

$client = new Hypertable_ThriftClient("localhost", 15867);
$namespace = $client->namespace_open("test");

echo "HQL examples\n";
print_r($client->hql_query($namespace, "show tables"));
print_r($client->hql_query($namespace, "select * from thrift_test revs=1 "));

echo "mutator examples\n";
$mutator = $client->mutator_open($namespace, "thrift_test", 0, 0);

$key = new Hypertable_ThriftGen_Key(array('row'=> 'php-k1', 'column_family'=> 'col'));
$cell = new Hypertable_ThriftGen_Cell(array('key' => $key, 'value'=> 'php-v1'));
$client->mutator_set_cell($mutator, $cell);
$client->mutator_close($mutator);

echo "shared mutator examples\n";
$mutate_spec = new Hypertable_ThriftGen_MutateSpec(array('appname'=>"test-php", 
                                                         'flush_interval'=>1000, 
                                                         'flags'=> 0));

$key = new Hypertable_ThriftGen_Key(array('row'=> 'php-put-k1', 'column_family'=> 'col'));
$cell = new Hypertable_ThriftGen_Cell(array('key' => $key, 'value'=> 'php-put-v1'));
$client->shared_mutator_set_cell($namespace, "thrift_test", $mutate_spec, $cell);

$key = new Hypertable_ThriftGen_Key(array('row'=> 'php-put-k2', 'column_family'=> 'col'));
$cell = new Hypertable_ThriftGen_Cell(array('key' => $key, 'value'=> 'php-put-v2'));
$client->shared_mutator_refresh($namespace, "thrift_test", $mutate_spec);
$client->shared_mutator_set_cell($namespace, "thrift_test", $mutate_spec, $cell);
sleep(2);

echo "scanner examples\n";
$scanner = $client->scanner_open($namespace, "thrift_test",
    new Hypertable_ThriftGen_ScanSpec(array('revs'=> 1)));

$cells = $client->scanner_get_cells($scanner);

while (!empty($cells)) {
  print_r($cells);
  $cells = $client->scanner_get_cells($scanner);
}
$client->scanner_close($scanner);
$client->namespace_close($namespace);

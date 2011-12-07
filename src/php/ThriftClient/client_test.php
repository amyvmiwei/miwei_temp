<?php
if (!isset($GLOBALS['THRIFT_ROOT']))
    $GLOBALS['THRIFT_ROOT'] = getenv('PHPTHRIFT_ROOT');

require_once dirname(__FILE__).'/ThriftClient.php';

$client = new Hypertable_ThriftClient("localhost", 38080);
$namespace = $client->namespace_open("test");

echo "HQL examples\n";
print_r($client->hql_query($namespace, "show tables"));
print_r($client->hql_query($namespace, "select * from thrift_test revs=1 "));

echo "mutator examples\n";
$mutator = $client->mutator_open($namespace, "thrift_test", 0, 0);

$key = new Key(array('row'=> 'php-k1', 'column_family'=> 'col'));
$cell = new Cell(array('key' => $key, 'value'=> 'php-v1'));
$client->mutator_set_cell($mutator, $cell);
$client->mutator_close($mutator);

echo "shared mutator examples\n";
$mutate_spec = new MutateSpec(array('appname'=>"test-php", 
                                                         'flush_interval'=>1000, 
                                                         'flags'=> 0));

$key = new Key(array('row'=> 'php-put-k1', 'column_family'=> 'col'));
$cell = new Cell(array('key' => $key, 'value'=> 'php-put-v1'));
$client->offer_cell($namespace, "thrift_test", $mutate_spec, $cell);

$key = new Key(array('row'=> 'php-put-k2', 'column_family'=> 'col'));
$cell = new Cell(array('key' => $key, 'value'=> 'php-put-v2'));
$client->refresh_shared_mutator($namespace, "thrift_test", $mutate_spec);
$client->offer_cell($namespace, "thrift_test", $mutate_spec, $cell);
sleep(2);

echo "scanner examples\n";
$scanner = $client->scanner_open($namespace, "thrift_test",
    new ScanSpec(array('revs'=> 1)));

$cells = $client->scanner_get_cells($scanner);

while (!empty($cells)) {
  print_r($cells);
  $cells = $client->scanner_get_cells($scanner);
}
$client->namespace_close($namespace);

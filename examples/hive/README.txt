This README describes how to run a simple example of the Hive Hypertable integration
For more info on this feature please refer to http://code.google.com/p/hypertable/wiki/HiveIntegration

1. Run the Hypertable commands in ht_commands.hql via:
$HT_HOME/bin/ht shell --command-file ht_commands_1.hql

2. Now run the Hive commands in hive_commands.q via:
$HIVE_HOME/bin/hive --auxpath $HT_HOME/lib/java/hypertable-0.9.5.0.pre4.jar -f hive_commands_1.q 

You can also run the commands in ht_commands_$i.hql and hive_commands_$i.q manually via the 
respective shells command shells.

CREATE EXTERNAL TABLE user_games (uid INT, chess MAP<STRING, INT>, tiddlywinks MAP<STRING, INT>) stored by 'org.hypertable.hadoop.hive.HTStorageHandler' with serdeproperties ("hypertable.columns.mapping" =":key,chess:,tiddlywinks:") TBLPROPERTIES ("hypertable.table.name"="user_games", "hypertable.table.namespace"="/");
SELECT * FROM user_games;
SELECT * FROM user_games WHERE uid > 12;
SELECT chess['kasparov'] FROM user_games;
CREATE TABLE hive_user_games(uid INT, chess STRING, chess_game_id INT, tiddlywinks STRING, tiddlywinks_game_id INT);
LOAD DATA LOCAL INPATH './kv_ht_3_1.txt' OVERWRITE INTO TABLE hive_user_games;
INSERT OVERWRITE TABLE user_games SELECT uid, map(chess, chess_game_id), map(tiddlywinks, tiddlywinks_game_id) FROM hive_user_games;
SELECT * FROM user_games WHERE (uid = "2" OR uid = "10");
CREATE TABLE games (id INT, state STRING);
LOAD DATA LOCAL INPATH './kv_ht_3_2.txt' OVERWRITE INTO TABLE games;

FROM user_games a JOIN games b ON (a.chess['kasparov'] = b.id) SELECT a.uid, b.state; 

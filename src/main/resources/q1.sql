-- -- 开启 mini-batch
-- SET table.exec.mini-batch.enabled=true;
-- -- mini-batch的时间间隔，即作业需要额外忍受的延迟
-- SET table.exec.mini-batch.allow-latency=1s;
-- -- 一个 mini-batch 中允许最多缓存的数据
-- SET table.exec.mini-batch.size=1000;
-- -- 开启 local-global 优化
-- SET table.optimizer.agg-phase-strategy=TWO_PHASE;
--
-- -- 开启 distinct agg 切分
-- SET table.optimizer.distinct-agg.split.enabled=true;


-- source
CREATE TABLE mysql_users1 (
                             id BIGINT PRIMARY KEY NOT ENFORCED ,
                             name STRING,
                             birthday TIMESTAMP(3),
                             ts TIMESTAMP(3)
) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = '10.130.5.61',
      'port' = '3306',
      'username' = 'root',
      'password' = 'infoCenter.2020',
      'server-time-zone' = 'Asia/Shanghai',
      'database-name' = 'flinksql',
      'table-name' = 'users'
      );
-- sink
CREATE TABLE ly1(
   id int PRIMARY KEY NOT ENFORCED,
   name string,
   `partition` string
 )
 PARTITIONED BY (`partition`)
 WITH (
   'connector' = 'hudi',
   'path' = 'hdfs://bigdata/tmp/hudi/t6',
   'write.tasks' = '2',
   'compaction.tasks' = '2',
   'table.type' = 'MERGE_ON_READ'
);

INSERT INTO ly1 select id,name,name as partition  from mysql_users;

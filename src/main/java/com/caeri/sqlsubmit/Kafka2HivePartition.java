package com.caeri.sqlsubmit;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;

public class Kafka2HivePartition {

    private final static Logger logger = LoggerFactory.getLogger(Kafka2HivePartition.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        //设置statebackend
      /*  env.setStateBackend(new MemoryStateBackend());

        CheckpointConfig config = env.getCheckpointConfig();

        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置checkpoint的周期, 每隔1000 ms进行启动一个检查点
        config.setCheckpointInterval(10000);

        // 设置模式为exactly-once
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        config.setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        config.setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        config.setMaxConcurrentCheckpoints(1);*/


        HiveConf conf = new HiveConf();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);
       tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));
        HiveCatalog catalog = new HiveCatalog(
                "myhive",
                "flink",
                conf,
                null

        );
       /* tableEnv.registerCatalog("myhive", catalog);
        tableEnv.useCatalog("myhive");

        tableEnv.executeSql("DROP TABLE IF EXISTS flink.test_kafka_source_ly12");
        tableEnv.executeSql("CREATE TABLE flink.test_kafka_source_ly12 (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  statdate STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'kafka_test1',\n" +
                " 'properties.bootstrap.servers' = 'master:6667',\n" +
                " 'properties.group.id' = 'hive_test_ly1',\n" +
                " 'format' = 'json',\n"+
                " 'scan.startup.mode' = 'latest-offset')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("DROP TABLE IF EXISTS flink.test_hive_sink_ly12");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS flink.test_hive_sink_ly12 (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT\n" +
                ") PARTITIONED BY (dt String, `hour` String) STORED AS PARQUET\n" +
                "TBLPROPERTIES (\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $hour:00:00',\n" +
                "  'sink.partition-commit.trigger' = 'process-time',\n" +
                "  'sink.partition-commit.delay'='0s',\n" +
                "  'sink.partition-commit.policy.kind' = 'metastore,success-file',\n" +
                "  'auto-compaction'='true',"+
                "  'compaction.file-size'='1MB'"+
                ")");
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("INSERT INTO flink.test_hive_sink_ly12 SELECT id,name,age,DATE_FORMAT(statdate,'yyyy-MM-dd'),DATE_FORMAT(statdate, 'HH') FROM flink.test_kafka_source_ly12");*/
    }
}

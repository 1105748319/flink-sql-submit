package com.caeri.sqlsubmit;

import com.caeri.sqlsubmit.cli.SqlCommandParser;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import sun.misc.BASE64Decoder;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqlSubmit {
    public static void main(String[] args) throws Exception {

//String argsss = "Q1JFQVRFIFRBQkxFIGZsaW5rLnRlc3Rfa2Fma2Ffc291cmNlX2x5MTIgKAogICAgICAgICAgICAgICAgICBpZCBJTlQsCiAgICAgICAgICAgICAgICAgIG5hbWUgU1RSSU5HLAogICAgICAgICAgICAgICAgICBhZ2UgSU5ULAogICAgICAgICAgICAgICAgICBzdGF0ZGF0ZSBTVFJJTkcKICAgICAgICAgICAgICAgICkgV0lUSCAoCiAgICAgICAgICAgICAgICAgJ2Nvbm5lY3RvcicgPSAna2Fma2EnLAogICAgICAgICAgICAgICAgICd0b3BpYycgPSAna2Fma2FfdGVzdDEnLAogICAgICAgICAgICAgICAgICdwcm9wZXJ0aWVzLmJvb3RzdHJhcC5zZXJ2ZXJzJyA9ICdtYXN0ZXI6NjY2NycsCiAgICAgICAgICAgICAgICAgJ3Byb3BlcnRpZXMuZ3JvdXAuaWQnID0gJ2hpdmVfdGVzdF9seTEyJywKICAgICAgICAgICAgICAgICAnZm9ybWF0JyA9ICdqc29uJywKICAgICAgICAgICAgICAgICAnc2Nhbi5zdGFydHVwLm1vZGUnID0gJ2xhdGVzdC1vZmZzZXQnKTsKCQkJCSAKc2V0IHRhYmxlLnNxbC1kaWFsZWN0PWhpdmU7CQkJCSAKQ1JFQVRFIFRBQkxFIElGIE5PVCBFWElTVFMgZmxpbmsudGVzdF9oaXZlX3NpbmtfbHkxMiAoCiAgICAgICAgICAgICAgICAgIGlkIElOVCwKICAgICAgICAgICAgICAgICAgbmFtZSBTVFJJTkcsCiAgICAgICAgICAgICAgICAgIGFnZSBJTlQKICAgICAgICAgICAgICAgICkgUEFSVElUSU9ORUQgQlkgKGR0IFN0cmluZywgYGhvdXJgIFN0cmluZykgU1RPUkVEIEFTIFBBUlFVRVQKICAgICAgICAgICAgICAgIFRCTFBST1BFUlRJRVMgKAogICAgICAgICAgICAgICAgICAncGFydGl0aW9uLnRpbWUtZXh0cmFjdG9yLnRpbWVzdGFtcC1wYXR0ZXJuJz0nJGR0ICRob3VyOjAwOjAwJywKICAgICAgICAgICAgICAgICAgJ3NpbmsucGFydGl0aW9uLWNvbW1pdC50cmlnZ2VyJyA9ICdwcm9jZXNzLXRpbWUnLAogICAgICAgICAgICAgICAgICAnc2luay5wYXJ0aXRpb24tY29tbWl0LmRlbGF5Jz0nMHMnLAogICAgICAgICAgICAgICAgICAnc2luay5wYXJ0aXRpb24tY29tbWl0LnBvbGljeS5raW5kJyA9ICdtZXRhc3RvcmUsc3VjY2Vzcy1maWxlJywKICAgICAgICAgICAgICAgICAgJ2F1dG8tY29tcGFjdGlvbic9J3RydWUnLAogICAgICAgICAgICAgICAgICAnY29tcGFjdGlvbi5maWxlLXNpemUnPScxTUInCiAgICAgICAgICAgICAgICApOwpzZXQgdGFibGUuc3FsLWRpYWxlY3Q9REVGQVVMVDsJCQkJCklOU0VSVCBJTlRPIGZsaW5rLnRlc3RfaGl2ZV9zaW5rX2x5MTIgU0VMRUNUIGlkLG5hbWUsYWdlLERBVEVfRk9STUFUKHN0YXRkYXRlLCd5eXl5LU1NLWRkJyksREFURV9GT1JNQVQoc3RhdGRhdGUsICdISCcpIEZST00gZmxpbmsudGVzdF9rYWZrYV9zb3VyY2VfbHkxMjs=";
 for (int i = 0; i < args.length; i++) {
            System.out.println("*************************" + args[i]);
        }
        if (args.length > 0) {
            BASE64Decoder decoder = new BASE64Decoder();
            //byte[] bytes = decoder.decodeBuffer(argsss);
            byte[] bytes = decoder.decodeBuffer(args[0]);

            String[] separated = new String(bytes).split("\n");
            for (int i = 0; i < separated.length; i++) {
                System.out.println(separated[i]+"====================");
            }

            List<String> list = Stream.of(separated).collect(Collectors.toList());
            SqlSubmit submit = new SqlSubmit(list);
            submit.run();
        }else {
            throw new RuntimeException("The parameter is abnormal, the parameter should be greater than or equal to one"+ Arrays.toString(args)+"+++++++"+args.length);
        }
    }

    // --------------------------------------------------------------------------------------------
    private List<String> sql;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    private SqlSubmit(List<String> list) {
        this.sql = list;
    }

    private void run() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));
        HiveCatalog catalog = new HiveCatalog(
                "myhive",
                "flink",
                "D:\\zhongqi-project\\flink-sql-submit\\src\\main\\resources",
                "D:\\zhongqi-project\\flink-sql-submit\\src\\main\\resources",
                null

        );
        tEnv.registerCatalog("myhive", catalog);
        tEnv.useCatalog("myhive");
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandParser.SqlCommandCall call : calls) {
            callCommand(call);
        }
        //tEnv.execute("SQL Job");
    }
    // --------------------------------------------------------------------------------------------
    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            System.out.println("CreateTable"+ddl);
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            System.out.println("callInsertInto"+dml);
            tEnv.executeSql(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }
}

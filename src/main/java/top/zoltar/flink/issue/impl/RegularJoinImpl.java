package top.zoltar.flink.issue.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.zoltar.flink.issue.bean.JoinRecord;
import top.zoltar.flink.issue.config.ConfigConstant;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created by wangfeifan on 2021/4/1.
 */

public class RegularJoinImpl implements JoinImpl {
    final private static Logger logger = LoggerFactory.getLogger(RegularJoinImpl.class);


    @Override
    public void doJoin(StreamExecutionEnvironment env,
                       DataStream<JoinRecord> firstStream,
                       DataStream<JoinRecord> secondStream){
        logger.info("immediate join test , use regular join");

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(env, fsSettings);

        logger.info("idleStateRetention : {}", ConfigConstant.IDLE_STATE_RETENTION);

        fsTableEnv.getConfig().setIdleStateRetention(Duration.ofMillis(ConfigConstant.IDLE_STATE_RETENTION));

        Table t1 = fsTableEnv.fromDataStream(firstStream, $("id"), $("ts"), $("text"));
        Table t2 = fsTableEnv.fromDataStream(secondStream, $("id"), $("ts"), $("text"));

        fsTableEnv.createTemporaryView("t1", t1);
        fsTableEnv.createTemporaryView("t2", t2);

        String sql = getSql();

        logger.info("regular join sql : {}", sql);

        Table joinedTable = fsTableEnv.sqlQuery(sql);

        fsTableEnv.toRetractStream(joinedTable, Row.class);
    }

    private String getSql(){
        return "SELECT\n" +
                "    t1.id as lid,\n" +
                "    t2.id as rid,\n" +
                "    t1.ts as lts,\n" +
                "    t2.ts as rts,\n" +
                "    t1.text as ltext,\n" +
                "    t2.text as rtext,\n" +
                "    CURRENT_TIMESTAMP as cts\n" +
                "FROM\n" +
                "    t1 LEFT JOIN t2 ON t1.id = t2.id"
                ;
    }
}

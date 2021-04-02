package top.zoltar.flink.issue.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.zoltar.flink.issue.bean.JoinRecord;

/**
 * Created by wangfeifan on 2021/4/1.
 */

public interface JoinImpl {

    void doJoin(StreamExecutionEnvironment env,
                DataStream<JoinRecord> firstStream,
                DataStream<JoinRecord> secondStream);

}

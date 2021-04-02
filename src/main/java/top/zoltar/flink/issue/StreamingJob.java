package top.zoltar.flink.issue;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.zoltar.flink.issue.bean.JoinRecord;
import top.zoltar.flink.issue.config.ConfigConstant;
import top.zoltar.flink.issue.function.MockJoinSource;
import top.zoltar.flink.issue.impl.JoinImpl;
import top.zoltar.flink.issue.impl.RegularJoinImpl;
import top.zoltar.flink.issue.join.util.SourceWrappedDataRecord;

import java.time.Duration;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// checkpoint settings
		configCheckpoint(env);

		DataStream<SourceWrappedDataRecord<String, JoinRecord, JoinRecord>> inputStream = env
				.addSource(new MockJoinSource()).name("mockJoinSource")
				.assignTimestampsAndWatermarks(
						WatermarkStrategy
								.<SourceWrappedDataRecord<String, JoinRecord, JoinRecord>>
										forBoundedOutOfOrderness(Duration.ofMinutes(3))
								.withTimestampAssigner((event, timestamp) -> event.getTimestamp())
				);

		DataStream<JoinRecord> firstStream = inputStream
				.filter(e -> e.isFirst()).name("firstStreamFilter")
				.map(e -> e.getFirstRecord()).name("toFirstRecordMap");
		DataStream<JoinRecord> secondStream = inputStream
				.filter(e -> !e.isFirst()).name("secondStreamFilter")
				.map(e -> e.getSecondRecord()).name("toSecondRecordMap");


		JoinImpl join = new RegularJoinImpl();
		join.doJoin(env, firstStream, secondStream);

		env.execute("flink-21986-regular-join-test-case");
	}

	private static void configCheckpoint(StreamExecutionEnvironment env){
		env.enableCheckpointing(ConfigConstant.CK_INTERVAL);
		CheckpointConfig conf = env.getCheckpointConfig();
		conf.setMinPauseBetweenCheckpoints(ConfigConstant.CK_MIN_PAUSE);
		conf.setCheckpointTimeout(ConfigConstant.CK_TIMEOUT);
		conf.setMaxConcurrentCheckpoints(ConfigConstant.CK_MAX_CONCURRENT);
		conf.setCheckpointingMode(CheckpointingMode.valueOf(ConfigConstant.CK_MODE));
	}

}

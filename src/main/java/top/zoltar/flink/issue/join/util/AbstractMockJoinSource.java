package top.zoltar.flink.issue.join.util;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.zoltar.flink.issue.config.ConfigConstant;
import top.zoltar.flink.issue.util.GsonUtil;
import top.zoltar.flink.issue.util.ParamParserUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by wangfeifan on 2020/11/26.
 */

public abstract class AbstractMockJoinSource<K, T1 extends DataRecord<K>, T2 extends DataRecord<K>>
        extends RichSourceFunction<SourceWrappedDataRecord<K, T1, T2>>
        implements ParallelSourceFunction<SourceWrappedDataRecord<K, T1, T2>> {

    final private static Logger logger = LoggerFactory.getLogger(AbstractMockJoinSource.class);

    final private static int BATCH_INTERVAL = 100;      // milliseconds
    final private static double DEFAULT_SIGMA = 0.3;    // 默认高斯分布的sigma

    private int speed;                                  // 每秒生产first stream元素的数量
    private double joinRate1;                           // first stream中join上的占比
    final private double joinRate2 = 1.0;               // second stream中join上的占比(只考虑衍生流全部能join上的情况)
    private List<SecRecDistSeg> secRecDistSegs;         // 同key落后时间分布

    private PriorityQueue<TimedKey<K>> delayedKeys;     // 需要延迟发出的second stream中的key

    private volatile boolean stop = false;

    private transient com.codahale.metrics.Histogram sameKeyDelayTime;


    private static class TimedKey<K> implements Comparable<TimedKey<K>>{
        public long triggerTime;
        public K key;

        public TimedKey(long triggerTime, K key){
            this.triggerTime = triggerTime;
            this.key = key;
        }

        @Override
        public int compareTo(TimedKey<K> o){
            long x = this.triggerTime;
            long y = o.triggerTime;
            return (x < y) ? -1 : ((x == y) ? 0 : 1);
        }
    }

    /**
     * Second Record distribution segment
     * 用于描述"同key落后时间"中的一段
     */
    protected static class SecRecDistSeg{
        public Duration duration;
        public double rate;

        public SecRecDistSeg(Duration duration, double rate){
            this.duration = duration;
            this.rate = rate;
        }
    }


    @Override
    public void open(Configuration parameters) {
        this.speed = ConfigConstant.PER_SOURCE_SPEED;

        this.joinRate1 = ConfigConstant.JOIN_RATE;

        this.secRecDistSegs = getSecRecDistSegs(ConfigConstant.SEC_REC_DISTSEGS);
        this.delayedKeys = new PriorityQueue<>(this.speed * 60 * 10);

        this.sameKeyDelayTime = new com.codahale.metrics.Histogram(new SlidingWindowReservoir(1000));

        // 同key落后时间关注的几个百分位
        getRuntimeContext().getMetricGroup().gauge("sameKeyDelayTime_p1000",
                (Gauge<Double>) () -> sameKeyDelayTime.getSnapshot().getValue(1));
        getRuntimeContext().getMetricGroup().gauge("sameKeyDelayTime_p950",
                (Gauge<Double>) () -> sameKeyDelayTime.getSnapshot().get95thPercentile());
        getRuntimeContext().getMetricGroup().gauge("sameKeyDelayTime_p925",
                (Gauge<Double>) () -> sameKeyDelayTime.getSnapshot().getValue(0.925));
        getRuntimeContext().getMetricGroup().gauge("sameKeyDelayTime_p900",
                (Gauge<Double>) () -> sameKeyDelayTime.getSnapshot().getValue(0.9));
        getRuntimeContext().getMetricGroup().gauge("sameKeyDelayTime_p800",
                (Gauge<Double>) () -> sameKeyDelayTime.getSnapshot().getValue(0.8));

        logger.info("abstractMockJoinSource : {}", GsonUtil.toJson(this));
    }

    private List<SecRecDistSeg> getSecRecDistSegs(String confStr){
        logger.info("secRecDistSegs config str : {}" , confStr);
        List<SecRecDistSeg> segs = new ArrayList<>(3);
        for(String item : confStr.split("_+")){
            String[] segStr = item.split(",");
            long duration = ParamParserUtil.parseTime(segStr[0]);
            double rate = Double.valueOf(segStr[1]);
            segs.add(new SecRecDistSeg(Duration.ofMillis(duration), rate));
        }
        return segs;
    }

    public void run(SourceContext<SourceWrappedDataRecord<K, T1, T2>> ctx) throws Exception{
        long totalCount = 0;
        long startTime = System.currentTimeMillis();

        while( ! stop ){
            long currentTime = System.currentTimeMillis();
            double targetCount = (currentTime - startTime) / 1000.0 * speed;
            int batchSize = (int) (targetCount - totalCount);

            long runtime = emitData(ctx, batchSize);

            totalCount += batchSize;
            if( runtime < BATCH_INTERVAL ) Thread.sleep(BATCH_INTERVAL);
        }
    }

    /**
     * emit amount of records
     * @param ctx
     * @param amount
     * @return runtime with milliseconds
     */
    private long emitData(SourceContext<SourceWrappedDataRecord<K, T1, T2>> ctx, int amount){
        long startTime = System.nanoTime();
        for(int i=0 ; i<amount ; i++){
            K key = genRandomKey();
            ctx.collect(new SourceWrappedDataRecord<>(true, genRandomFirstRecordOnKey(key)));

            // 按first stream中join上的占比来决定该key要不要生产对应的second stream数据
            if( ThreadLocalRandom.current().nextDouble(1) < joinRate1 ){
                createDelayedSecondRecordTask(key);
            }

            emitTriggeredSecondRecord(ctx);
        }
        long endTime = System.currentTimeMillis();
        return (endTime-startTime) / 1000000;   // to millisec
    }

    private void createDelayedSecondRecordTask(K key){
        long currentTime = System.currentTimeMillis();
        double passedSegRate = 0;
        long passedSegDuration = 0;
        double position = ThreadLocalRandom.current().nextDouble(1);
        for(SecRecDistSeg distSeg : secRecDistSegs){
            if( position < distSeg.rate + passedSegRate){
                double offsetPosition = Math.abs(ThreadLocalRandom.current().nextGaussian()*DEFAULT_SIGMA) % 1;
                long offsetTime = (long) (offsetPosition * distSeg.duration.toMillis());
                long triggerTime = currentTime + passedSegDuration + offsetTime;

                delayedKeys.offer(new TimedKey<>(triggerTime, key));
                sameKeyDelayTime.update(triggerTime - currentTime);
                break;
            }
            passedSegRate += distSeg.rate;
            passedSegDuration += distSeg.duration.toMillis();
        }
    }

    private void emitTriggeredSecondRecord(SourceContext<SourceWrappedDataRecord<K, T1, T2>> ctx){
        while( delayedKeys.peek() != null && delayedKeys.peek().triggerTime <= System.currentTimeMillis() ) {
            TimedKey<K> peek = delayedKeys.poll();
            ctx.collect(new SourceWrappedDataRecord<>(false, genRandomSecondRecordOnKey(peek.key)));
            if( joinRate2 < 1 ){
                // 这种方式处理second stream join率的问题时要求 joinRate2 > 50%
                double dice = ThreadLocalRandom.current().nextDouble(1);
                if( dice < (1-joinRate2)/joinRate2 ){
                    ctx.collect(new SourceWrappedDataRecord<>(false, genRandomSecondRecordOnKey(genRandomKey())));
                }
            }
        }
    }

    @Override
    public void close() {
        this.stop = true;
    }

    @Override
    public void cancel(){
        this.stop = true;
    }


    protected abstract K genRandomKey();
    protected abstract T1 genRandomFirstRecordOnKey(K key);
    protected abstract T2 genRandomSecondRecordOnKey(K key);

}

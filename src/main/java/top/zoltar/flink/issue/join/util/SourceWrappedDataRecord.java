package top.zoltar.flink.issue.join.util;

/**
 * Created by wangfeifan on 2020/11/26.
 */

public class SourceWrappedDataRecord<K, T1 extends DataRecord<K>, T2 extends DataRecord<K>>{
    private boolean first;
    private T1 firstRecord;
    private T2 secondRecord;
    private long timestamp;

    public SourceWrappedDataRecord(boolean isFirst, DataRecord<K> record){
        this.timestamp = System.currentTimeMillis();

        this.first = isFirst;

        if( isFirst ){
            this.firstRecord = (T1) record;
        } else {
            this.secondRecord = (T2) record;
        }
    }

    public boolean isFirst(){
        return first;
    }

    public T1 getFirstRecord(){
        return firstRecord;
    }

    public T2 getSecondRecord(){
        return secondRecord;
    }

    public long getTimestamp(){
        return timestamp;
    }

}

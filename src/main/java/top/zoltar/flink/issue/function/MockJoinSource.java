package top.zoltar.flink.issue.function;

import org.apache.flink.configuration.Configuration;
import top.zoltar.flink.issue.bean.JoinRecord;
import top.zoltar.flink.issue.config.ConfigConstant;
import top.zoltar.flink.issue.join.util.AbstractMockJoinSource;

import java.util.UUID;

/**
 * Created by wangfeifan on 2021/4/1.
 */
public class MockJoinSource extends AbstractMockJoinSource<String, JoinRecord, JoinRecord> {

    private String text1;
    private String text2;


    @Override
    public void open(Configuration parameters){
        super.open(parameters);

        this.text1 = genPlaceholderString(ConfigConstant.TEXT1LENGTH - 44);
        this.text2 = genPlaceholderString(ConfigConstant.TEXT2LENGTH - 44);
    }

    @Override
    protected String genRandomKey(){
        return UUID.randomUUID().toString();
    }

    @Override
    protected JoinRecord genRandomFirstRecordOnKey(String key){
        return genJoinRecord(key, text1);
    }

    @Override
    protected JoinRecord genRandomSecondRecordOnKey(String key){
        return genJoinRecord(key, text2);
    }

    private JoinRecord genJoinRecord(String key, String text){
        JoinRecord record = new JoinRecord();
        record.setId(key);
        record.setTs(System.currentTimeMillis());
        record.setText(text);
        return record;
    }

    private static String genPlaceholderString(int length){
        StringBuilder sb = new StringBuilder();

        for(int i=0 ; i<length ; i++){
            sb.append("*");
        }

        return sb.toString();
    }
}

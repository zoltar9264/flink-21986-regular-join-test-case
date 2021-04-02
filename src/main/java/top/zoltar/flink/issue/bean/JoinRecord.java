package top.zoltar.flink.issue.bean;

import lombok.Data;
import top.zoltar.flink.issue.join.util.DataRecord;

/**
 * Created by wangfeifan on 2021/4/1.
 */

@Data
public class JoinRecord implements DataRecord<String> {
    private String id;
    private long ts;
    private String text;
}

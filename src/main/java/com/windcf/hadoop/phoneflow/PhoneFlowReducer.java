package com.windcf.hadoop.phoneflow;

import com.windcf.hadoop.bean.PhoneFlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-11 19:06
 * @package com.windcf.hadoop.writable
 * @description TODO
 */
public class PhoneFlowReducer extends Reducer<Text, PhoneFlowBean, Text, PhoneFlowBean> {
    @Override
    protected void reduce(Text key, Iterable<PhoneFlowBean> values, Reducer<Text, PhoneFlowBean, Text, PhoneFlowBean>.Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        for (PhoneFlowBean value : values) {
            up += value.getUpFlow().get();
            down += value.getDownFlow().get();
        }
        context.write(key, new PhoneFlowBean(key.toString(), up, down));
    }
}

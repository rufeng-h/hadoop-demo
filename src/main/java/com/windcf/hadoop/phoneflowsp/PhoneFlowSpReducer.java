package com.windcf.hadoop.phoneflowsp;

import com.windcf.hadoop.bean.PhoneFlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-12 10:55
 * @package com.windcf.hadoop.phoneflowsp
 * @description TODO
 */
public class PhoneFlowSpReducer extends Reducer<PhoneFlowBean, Text, Text, PhoneFlowBean> {
    @Override
    protected void reduce(PhoneFlowBean key, Iterable<Text> values, Reducer<PhoneFlowBean, Text, Text, PhoneFlowBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}

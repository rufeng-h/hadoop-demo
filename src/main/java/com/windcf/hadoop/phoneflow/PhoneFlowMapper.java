package com.windcf.hadoop.phoneflow;

import com.windcf.hadoop.bean.PhoneFlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-11 18:58
 * @package com.windcf.hadoop.writable
 * @description TODO
 */
public class PhoneFlowMapper extends Mapper<LongWritable, Text, Text, PhoneFlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PhoneFlowBean>.Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s+?");
        int len = words.length;
        long upFlow = Long.parseLong(words[len - 2]);
        long downFlow = Long.parseLong(words[len - 3]);
        context.write(new Text(words[1]), new PhoneFlowBean(words[1], upFlow, downFlow));
    }
}

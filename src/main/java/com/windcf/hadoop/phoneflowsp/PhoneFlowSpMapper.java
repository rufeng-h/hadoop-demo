package com.windcf.hadoop.phoneflowsp;

import com.windcf.hadoop.bean.PhoneFlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-12 10:48
 * @package com.windcf.hadoop.phoneflowsp
 * @description TODO
 */
public class PhoneFlowSpMapper extends Mapper<LongWritable, Text, PhoneFlowBean, Text> {
    private final Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PhoneFlowBean, Text>.Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        long up = Long.parseLong(words[1]);
        long down = Long.parseLong(words[2]);
        text.set(words[0]);
        context.write(new PhoneFlowBean(words[0], up, down), text);
    }
}

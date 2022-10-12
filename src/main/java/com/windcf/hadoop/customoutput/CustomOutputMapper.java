package com.windcf.hadoop.customoutput;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-11 16:45
 * @package com.windcf.hadoop.wordcount
 * @description mapper
 */
public class CustomOutputMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private final Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s");
        for (String word : words) {
            text.set(word);
            context.write(text, NullWritable.get());
        }
    }
}

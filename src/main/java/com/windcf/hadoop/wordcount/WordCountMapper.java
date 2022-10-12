package com.windcf.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-11 16:45
 * @package com.windcf.hadoop.wordcount
 * @description mapper
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text text = new Text();
    private final IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            text.set(word);
            context.write(text, intWritable);
        }
    }
}

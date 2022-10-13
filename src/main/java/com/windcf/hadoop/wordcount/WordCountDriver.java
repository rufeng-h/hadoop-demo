package com.windcf.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.Files;

/**
 * @author chunf
 * @time 2022-10-11 16:45
 * @package com.windcf.hadoop.wordcount
 * @description TODO
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration configuration = new Configuration();
        // 开启 map 端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress", true);
        // 设置 map 端输出压缩方式
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountDriver.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    private static void deleteDirs(java.nio.file.Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(p -> {
                try {
                    deleteDirs(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        Files.delete(path);
    }
}

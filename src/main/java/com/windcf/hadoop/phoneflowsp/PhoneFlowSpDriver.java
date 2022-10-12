package com.windcf.hadoop.phoneflowsp;

import com.windcf.hadoop.bean.PhoneFlowBean;
import com.windcf.hadoop.phoneflow.PhoneFlowDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-12 10:47
 * @package com.windcf.hadoop.phoneflowsp
 * @description TODO
 */
public class PhoneFlowSpDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);


        job.setJarByClass(PhoneFlowDriver.class);
        job.setReducerClass(PhoneFlowSpReducer.class);
        job.setMapperClass(PhoneFlowSpMapper.class);

        job.setMapOutputKeyClass(PhoneFlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(PhoneFlowSpPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneFlowBean.class);


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

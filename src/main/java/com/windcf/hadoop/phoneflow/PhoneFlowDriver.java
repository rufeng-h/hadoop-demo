package com.windcf.hadoop.phoneflow;

import com.windcf.hadoop.bean.PhoneFlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-11 18:54
 * @package com.windcf.hadoop.writable
 * @description TODO
 */
public class PhoneFlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(PhoneFlowDriver.class);
        job.setReducerClass(PhoneFlowReducer.class);
        job.setMapperClass(PhoneFlowMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneFlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneFlowBean.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(PhoneFlowPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

package com.windcf.hadoop.orderjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * @author chunf
 * @time 2022-10-11 18:54
 * @package com.windcf.hadoop.writable
 * @description TODO
 */
public class OrderJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(OrderJoinDriver.class);
        job.setMapperClass(OrderJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(Objects.requireNonNull(OrderJoinDriver.class.getClassLoader().getResource("pd.txt")).toURI());

        job.setOutputFormatClass(OrderJoinOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

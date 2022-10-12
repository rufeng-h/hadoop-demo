package com.windcf.hadoop.customoutput;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author chunf
 * @time 2022-10-12 14:41
 * @package com.windcf.hadoop.customoutput
 * @description TODO
 */
public class CustomOutputPartition extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {
        return text.toString().contains("hadoop") ? 0 : 1;
    }
}

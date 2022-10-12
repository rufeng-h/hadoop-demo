package com.windcf.hadoop.phoneflowsp;

import com.windcf.hadoop.bean.PhoneFlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author chunf
 * @time 2022-10-12 11:01
 * @package com.windcf.hadoop.phoneflowsp
 * @description TODO
 */
public class PhoneFlowSpPartitioner extends Partitioner<PhoneFlowBean, Text> {
    @Override
    public int getPartition(PhoneFlowBean phoneFlowBean, Text text, int numPartitions) {
        String prefix = text.toString().substring(0, 3);
        switch (prefix) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}

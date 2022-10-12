package com.windcf.hadoop.phoneflow;

import com.windcf.hadoop.bean.PhoneFlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author chunf
 * @time 2022-10-12 10:07
 * @package com.windcf.hadoop.writable
 * @description TODO
 */
public class PhoneFlowPartitioner extends Partitioner<Text, PhoneFlowBean> {

    @Override
    public int getPartition(Text text, PhoneFlowBean phoneFlowBean, int numPartitions) {
        String prefix = phoneFlowBean.getPhone().toString().substring(0, 3);
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

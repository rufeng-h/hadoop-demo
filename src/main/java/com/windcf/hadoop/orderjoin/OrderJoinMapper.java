package com.windcf.hadoop.orderjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chunf
 * @time 2022-10-11 18:58
 * @package com.windcf.hadoop.writable
 * @description TODO
 */
public class OrderJoinMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
    private static final Log LOG = LogFactory.getLog(OrderJoinMapper.class);
    private static final Map<String, String> PROD_MAP = new HashMap<>();
    private final Text orderId = new Text();

    private int curLine = 0;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, OrderBean>.Context context) throws IOException {
        URI uri = context.getCacheFiles()[0];
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream inputStream = fs.open(new Path(uri));
        int lc = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                lc += 1;
                if (lc == 1) {
                    continue;
                }
                String[] words = line.split("\\s");
                PROD_MAP.put(words[0], words[1]);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, OrderBean>.Context context) throws IOException, InterruptedException {
        curLine += 1;
        if (curLine == 1) {
            return;
        }
        String[] words = value.toString().split("\\s");
        String prodName = PROD_MAP.get(words[1]);
        if (prodName == null) {
            throw new RuntimeException("error order id: " + words[1]);
        }
        OrderBean orderBean = new OrderBean(Long.parseLong(words[0]), words[1], Long.parseLong(words[2]), prodName);
        orderId.set(words[0]);
        context.write(orderId, orderBean);
    }
}

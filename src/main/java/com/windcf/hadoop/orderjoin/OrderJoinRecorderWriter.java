package com.windcf.hadoop.orderjoin;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

/**
 * @author chunf
 * @time 2022-10-13 9:18
 * @package com.windcf.hadoop.orderjoin
 * @description TODO
 */
public class OrderJoinRecorderWriter extends RecordWriter<Text, OrderBean> {
    private final FSDataOutputStream outputStream;

    public OrderJoinRecorderWriter(FSDataOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(Text key, OrderBean value) throws IOException, InterruptedException {
        String s = new StringJoiner("\t", "", "\n")
                .add(value.getOrderId().toString())
                .add(value.getAmount().toString())
                .add(value.getProdName().toString()).toString();
        outputStream.write(s.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        outputStream.close();
    }
}

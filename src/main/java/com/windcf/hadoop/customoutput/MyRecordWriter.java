package com.windcf.hadoop.customoutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-12 15:32
 * @package com.windcf.hadoop.customoutput
 * @description TODO
 */
public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    private final FSDataOutputStream outputStream;

    public MyRecordWriter(FSDataOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        outputStream.writeBytes(key.toString() + "\n");
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        outputStream.close();
    }
}

package com.windcf.hadoop.orderjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-13 10:28
 * @package com.windcf.hadoop.orderjoin
 * @description TODO
 */
public class OrderJoinRecordReader extends RecordReader<NullWritable, Text> {
    private SplitLineReader in;
    private NullWritable key;
    private Text value = new Text();

    private int maxLength = Integer.MAX_VALUE;
    private long start;
    private long end;
    private long cur;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        FileSystem fs = fileSplit.getPath().getFileSystem(context.getConfiguration());
        FSDataInputStream inputStream = fs.open(fileSplit.getPath());
        inputStream.seek(start);

        in = new SplitLineReader(inputStream, null);
        end = start + fileSplit.getLength();
        // If this is not the first split, we always throw away first record
        // because we always (except the last split) read one extra line in
        // next() method.
        if (start != 0) {
            start += in.readLine(new Text(), 0, maxLength);
        }
        cur = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        long maxToConsume = end - cur;
        if (maxToConsume > Integer.MAX_VALUE) {
            throw new RuntimeException("too large to read!");
        }
        /* max line length not set, default is Integer.MAX_VALUE */
        int size = in.readLine(value, Integer.MAX_VALUE, (int) maxToConsume);
        if (size == 0) {
            return false;
        }
        cur += size;
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return ((float) cur - start) / (end - start);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}

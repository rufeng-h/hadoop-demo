package com.windcf.hadoop.orderjoin;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chunf
 * @time 2022-10-13 10:26
 * @package com.windcf.hadoop.orderjoin
 * @description TODO
 */
public class OrderJoinInputFormat extends InputFormat<NullWritable, Text> {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        List<InputSplit> splits = new ArrayList<>();
        Path path = new Path(context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir"));
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
        while (files.hasNext()) {
            LocatedFileStatus fileStatus = files.next();
            splits.add(new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), new String[]{"localhost"}));
        }
        return splits;
    }

    @Override
    public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new OrderJoinRecordReader();
    }
}

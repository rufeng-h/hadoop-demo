package com.windcf.hadoop.orderjoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-13 9:15
 * @package com.windcf.hadoop.orderjoin
 * @description TODO
 */
public class OrderJoinOutputCommiter extends OutputCommitter {
    public OrderJoinOutputCommiter(Path outputPath, TaskAttemptContext context) throws IOException {
        outputPath.getFileSystem(context.getConfiguration()).mkdirs(outputPath);
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {

    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {

    }
}

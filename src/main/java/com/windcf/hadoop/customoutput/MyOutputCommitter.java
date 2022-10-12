package com.windcf.hadoop.customoutput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-12 15:09
 * @package com.windcf.hadoop.customoutput
 * @description TODO
 */
public class MyOutputCommitter extends OutputCommitter {
    public MyOutputCommitter(Path outputPath, JobContext context) throws IOException {
        outputPath.getFileSystem(context.getConfiguration()).mkdirs(outputPath);
    }

    @Override
    public void setupJob(JobContext jobContext) {

    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) {

    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) {

    }
}

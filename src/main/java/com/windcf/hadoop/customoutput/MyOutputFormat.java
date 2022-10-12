package com.windcf.hadoop.customoutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-12 15:32
 * @package com.windcf.hadoop.customoutput
 * @description TODO
 */
public class MyOutputFormat extends OutputFormat<Text, NullWritable> {
    private final static Log LOG = LogFactory.getLog(MyOutputFormat.class);

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException {
        TaskID taskId = job.getTaskAttemptID().getTaskID();
        char c = TaskID.getRepresentingCharacter(taskId.getTaskType());
        String s = String.valueOf(c) + taskId.getId();
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path(getOutputPath(job), s));
        return new MyRecordWriter(outputStream);
    }

    private Path getOutputPath(JobContext job) {
        String s = job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
        return s == null ? null : new Path(s);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        Configuration configuration = context.getConfiguration();
        configuration.forEach(e -> System.out.println(e.getKey() + " : " + e.getValue()));
        Path path = getOutputPath(context);
        if (path == null) {
            throw new IOException("output path can not be null");
        }
        if (path.getFileSystem(configuration).exists(path)) {
            LOG.warn("path: " + path + "already exists!");
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        Path outputPath = getOutputPath(context);
        return new MyOutputCommitter(outputPath, context);
    }
}

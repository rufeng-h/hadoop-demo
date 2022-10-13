package com.windcf.hadoop.orderjoin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-13 9:13
 * @package com.windcf.hadoop.orderjoin
 * @description TODO
 */
public class OrderJoinOutputFormat extends OutputFormat<Text, OrderBean> {
    private static final Log LOG = LogFactory.getLog(OrderJoinOutputFormat.class);

    @Override
    public RecordWriter<Text, OrderBean> getRecordWriter(TaskAttemptContext context) throws IOException {
        TaskID taskId = context.getTaskAttemptID().getTaskID();
        char c = TaskID.getRepresentingCharacter(taskId.getTaskType());
        String s = String.valueOf(c) + taskId.getId();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataOutputStream outputStream = fs.create(new Path(getOutputPath(context), s));
        return new OrderJoinRecorderWriter(outputStream);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        Path outputPath = getOutputPath(context);
        if (outputPath.getFileSystem(context.getConfiguration()).exists(outputPath)) {
            LOG.warn("output path already exists: " + outputPath);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return new OrderJoinOutputCommiter(getOutputPath(context), context);
    }

    private Path getOutputPath(JobContext job) {
        String s = job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
        if (s == null) {
            throw new RuntimeException("output path can not be null!");
        }
        return new Path(s);
    }
}

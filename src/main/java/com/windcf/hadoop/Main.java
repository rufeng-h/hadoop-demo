package com.windcf.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author chunf
 * @time 2022-10-11 14:28
 * @package com.windcf.hadoop
 * @description TODO
 */
public class Main {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop001:8020"), configuration, "hadoop");
        fs.copyToLocalFile(true, new Path("/duebass01.jpg"), new Path("D:/duebass.jpg"));
//        fs.copyFromLocalFile(false, true, new Path("D:\\fileRecv\\duebass\\duebass\\75614297gy1fgl17q6tynj22y21z9qv8.jpg"), new Path("/duebass01.jpg"));
        fs.close();
    }
}

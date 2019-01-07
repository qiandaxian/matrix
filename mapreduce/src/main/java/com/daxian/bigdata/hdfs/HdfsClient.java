package com.daxian.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {

        FileSystem fs =  FileSystem.get(new URI("hdfs://192.168.1.100:9000"),new Configuration(),"root");

        fs.copyFromLocalFile(new Path("C:/Users/qiandaxian/Downloads/廉政宣传视频.mp4"),new Path("/qiandaxian/"));

        fs.close();
    }
}

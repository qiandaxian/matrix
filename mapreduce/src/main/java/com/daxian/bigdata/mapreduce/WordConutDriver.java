package com.daxian.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordConutDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

       Job job = Job.getInstance();
       job.setJarByClass(WordConutDriver.class);
       job.setMapperClass(WordCountMapper.class);
       job.setReducerClass(WordCountReduce.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(IntWritable.class);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       //设置输入输出
       FileInputFormat.setInputPaths(job,new Path("/qiandaxian/hadoop/in/*"));
       FileOutputFormat.setOutputPath(job,new Path("/qiandaxian/hadoop/out/"));

       boolean rs = job.waitForCompletion(true);


        System.out.println(rs);
    }
}

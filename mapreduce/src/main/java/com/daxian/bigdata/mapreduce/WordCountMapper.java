package com.daxian.bigdata.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);

        String line = value.toString();

        System.out.println("map-key:"+key.toString());
        System.out.println("map-value:"+line);

        String[] words = line.split(" ");

        for (String word:words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}

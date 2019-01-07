package com.daxian.bigdata.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        System.out.println("reduce-key:"+key.toString());
        System.out.print("reduce-value:");

        for(IntWritable count: values){
            System.out.print(count+",");
            sum += count.get();
        }

        context.write(key,new IntWritable(sum));
    }
}

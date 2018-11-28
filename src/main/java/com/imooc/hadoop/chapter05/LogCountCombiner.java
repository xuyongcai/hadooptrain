package com.imooc.hadoop.chapter05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: xiaochai
 * @create: 2018-11-29
 **/
public class LogCountCombiner extends Reducer<MemberLogTime, IntWritable, MemberLogTime, IntWritable> {
    @Override
    protected void reduce(MemberLogTime key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable val : values){
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

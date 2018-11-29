package com.imooc.hadoop.chapter05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author: xiaochai
 * @create: 2018-11-29
 **/
public class LogCountPartitioner extends Partitioner<MemberLogTime, IntWritable> {
    public int getPartition(MemberLogTime key, IntWritable value, int numPartitions) {
        String date = key.getLogTime();
        if(date.contains("2016-01")){
            return 0 % numPartitions;
        }else {
            return 1 % numPartitions;
        }
    }
}

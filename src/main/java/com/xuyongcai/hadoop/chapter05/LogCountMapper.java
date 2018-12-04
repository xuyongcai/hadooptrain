package com.xuyongcai.hadoop.chapter05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: xiaochai
 * @create: 2018-11-29
 **/
public class LogCountMapper extends Mapper<Text, Text, MemberLogTime, IntWritable> {

    private MemberLogTime mt = new MemberLogTime();
    private IntWritable one = new IntWritable(1);

    enum LogCounter{
        January,
        February,
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] vals = value.toString().split(" ");
        String member_name = vals[0];
        String logTime = vals[1];

        if (logTime.contains("2016-01")){
            context.getCounter(LogCounter.January).increment(1);
        }else if (logTime.contains("2016-02")){
            context.getCounter(LogCounter.February).increment(1);
        }

        mt.setMember_name(member_name);
        mt.setLogTime(logTime);

        context.write(mt, one);
    }
}

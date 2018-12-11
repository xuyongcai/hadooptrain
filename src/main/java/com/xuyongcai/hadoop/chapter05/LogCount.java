package com.xuyongcai.hadoop.chapter05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 2.日志文件统计驱动类实现代码
 * @author: xiaochai
 * @create: 2018-11-29
 **/
public class LogCount {

    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 2){
            args = new String[2];
            args[0] = "hdfs://localhost:9000/user/root/user_login/JanFeb/part-m-00000";
            args[1] = "hdfs://localhost:9000/user/root/user_login/logcount";
        }

        //创建configuration
        Configuration configuration = new Configuration();

        //创建job
        Job job = Job.getInstance(configuration,"log_count");

        //设置job的处理类
        job.setJarByClass(LogCount.class);

        //设置map相关参数
        job.setMapperClass(LogCountMapper.class);
        job.setMapOutputKeyClass(MemberLogTime.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce相关参数
        job.setReducerClass(LogCountReducer.class);
        job.setOutputKeyClass(MemberLogTime.class);
        job.setOutputValueClass(IntWritable.class);

        //设置combiner类
        job.setCombinerClass(LogCountCombiner.class);

        //设置partitioner类
        job.setPartitionerClass(LogCountPartitioner.class);
        //设置reducer个数
        job.setNumReduceTasks(2);

        //设置作业处理的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 准备清理已存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, but is has deleted");
        }
        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}

package com.xuyongcai.hadoop.chapter05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1.筛选出1月、2月的用户登陆数据
 * @author: xiaochai
 * @create: 2018-12-11
 **/
public class SelectData {

    public static class SelectDataMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] vals = value.toString().split(",");

            //过滤选取1月、2月的用户登陆数据
            if (vals[1].contains("2016-01") || vals[1].contains("2016-02")){
                context.write(new Text(vals[0]), new Text(vals[1]));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        if (args.length != 2){
            args = new String[2];
            args[0] = "hdfs://localhost:9000/user/root/user_login/user_login.txt";
            args[1] = "hdfs://localhost:9000/user/root/user_login/JanFeb";
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(SelectData.class);

        //设置mapper相关
        job.setMapperClass(SelectDataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reducer相关
        job.setNumReduceTasks(0);

        //为job设置输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 准备清理已存在的输出目录
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, but is has deleted");
        }
        //为job设置输出路径
        FileOutputFormat.setOutputPath(job, outputPath);

        //运行job
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

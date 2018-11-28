package com.imooc.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 1.用户访问次数统计
 * @author: xiaochai
 * @create: 2018-11-28
 **/
public class DailyAccessCount {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

        private static final IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //读取一行
            String line = value.toString();
            String[] vals = line.split(",");

            //提取数组中的访问日期作为输出key
            String keyOutput = vals[1];

            context.write(new Text(keyOutput), one);
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0; //定义类加强，初始值为0

            for (IntWritable val : values){
                //将相同键的所有值进行累加
                sum += val.get();
            }
            result.set(sum);

            context.write(key, result);
        }
    }

    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建configuration
        Configuration configuration = new Configuration();

        //创建job
        Job job = Job.getInstance(configuration,"daily_access_count");

        //设置job的处理类
        job.setJarByClass(DailyAccessCount.class);

        //设置map相关参数
        job.setMapperClass(DailyAccessCount.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce相关参数
        job.setReducerClass(DailyAccessCount.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置作业处理的输入路径
        for (int i = 0; i < args.length - 1; i++){
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        // 准备清理已存在的输出目录
        Path outputPath = new Path(args[args.length - 1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath, true);
            System.out.println("output file exists, but is has deleted");
        }
        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

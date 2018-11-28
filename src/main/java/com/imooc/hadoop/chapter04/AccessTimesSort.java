package com.imooc.hadoop.chapter04;

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
 * 2.访问次数排序
 * @author: xiaochai
 * @create: 2018-11-28
 **/
public class AccessTimesSort {

    public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //读取一行
            String line = value.toString();
            //指定tab为分割符
            String[] vals = line.split("\t");

            //访问次数作为输出key
            Integer keyOutput = Integer.parseInt(vals[1]);

            String valueOutput = vals[0];

            context.write(new IntWritable(keyOutput), new Text(valueOutput));
        }
    }

    public static class MyReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //数据传到ruduce前，内部sorter已经按照key进行了排序，故只需要交换key和val输出就行。
            for (Text val : values){
                context.write(val, key);
            }
        }
    }

    /**
     * 定义Driver：封装了MapReduce作业的所有信息
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //创建configuration
        Configuration configuration = new Configuration();

        //创建job
        Job job = Job.getInstance(configuration,"access_times_sort");

        //设置job的处理类
        job.setJarByClass(AccessTimesSort.class);

        //设置map相关参数
        job.setMapperClass(AccessTimesSort.MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce相关参数
        job.setReducerClass(AccessTimesSort.MyReducer.class);
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

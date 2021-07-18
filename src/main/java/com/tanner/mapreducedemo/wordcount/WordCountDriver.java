package com.tanner.mapreducedemo.wordcount;

import com.sun.org.apache.xerces.internal.dom.PSVIAttrNSImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author zhang.t.c
 * @date 2021/7/18
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {

        // 1、获取配置信息，job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2、关联 driver
        job.setJarByClass(WordCountDriver.class);
        // 3、关联 mapper、reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4、设置 mapper 的输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 4、设置最终的输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6、设置输入和输出的路径
        // "D:\\demo\\mapreduce-demo\\data\\hello.txt"
        FileInputFormat.setInputPaths(job, new Path("D:\\demo\\mapreduce-demo\\data\\hello.txt"));
        // "D:\\demo\\mapreduce-demo\\output" (目录不能已存在)
        FileOutputFormat.setOutputPath(job, new Path("D:\\demo\\mapreduce-demo\\output"));
        // 7、提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}

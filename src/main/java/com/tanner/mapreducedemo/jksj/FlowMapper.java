package com.tanner.mapreducedemo.jksj;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhang.t.c
 * @date 2021/7/18
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
//        String timestamp = split[0];
        String phone = split[1];
//        String address = split[2];
//        String ip = split[3];
//        String domain = split[4];
//        String packet = split[5];
//        String receive = split[6];
        long upload = Long.parseLong(split[8]);
        long download = Long.parseLong(split[9]);
//        String response = split[9];

        FlowBean flowBean = new FlowBean(upload, download);
        k.set(phone);
        context.write(k, flowBean);

    }
}

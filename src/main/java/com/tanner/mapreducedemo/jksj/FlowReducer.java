package com.tanner.mapreducedemo.jksj;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhang.t.c
 * @date 2021/7/18
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    int upsum;
    int downsum;

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        upsum = 0;
        downsum = 0;
        for (FlowBean value : values) {
            upsum += value.getUpFlow();
            downsum += value.getDownFlow();
        }
        FlowBean flowBean = new FlowBean(upsum, downsum);
        context.write(key, flowBean);
    }
}

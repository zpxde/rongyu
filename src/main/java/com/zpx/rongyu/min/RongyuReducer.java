package com.zpx.rongyu.min;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RongyuReducer extends Reducer<Text, RongyuBean, Text, LongWritable> {



    @Override
    protected void reduce(Text key, Iterable<RongyuBean> values, Context context) throws IOException, InterruptedException {
           long min=100;
        for (RongyuBean value : values) {
            min=Math.min(min,value.getTemper());
        }
        context.write(key,new LongWritable(min));

    }
}
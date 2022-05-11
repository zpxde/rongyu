package com.zpx.rongyu.max;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RongyuReducer extends Reducer<Text, RongyuBean, Text, LongWritable> {



    @Override
    protected void reduce(Text key, Iterable<RongyuBean> values, Context context) throws IOException, InterruptedException {
           long max=0;
        for (RongyuBean value : values) {
            max=Math.max(max,value.getTemper());
        }
        context.write(key,new LongWritable(max));

    }
}
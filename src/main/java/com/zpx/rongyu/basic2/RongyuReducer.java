package com.zpx.rongyu.basic2;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RongyuReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable d=new DoubleWritable();


    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        for (DoubleWritable value : values) {
            if (value.get()<60.0||value.get()>60.046){
                d=value;
                context.write(key,d);
            }
        }



    }



}
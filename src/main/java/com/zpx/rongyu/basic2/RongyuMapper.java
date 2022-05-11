package com.zpx.rongyu.basic2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RongyuMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
    private Text outK = new Text();


    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String line = value.toString();

        String[] split = line.split("\t");

        String bianhao= split[0];
        String d=split[1];


        this.outK.set(bianhao);



        context.write(this.outK, new DoubleWritable(Double.parseDouble(d)));
    }
}

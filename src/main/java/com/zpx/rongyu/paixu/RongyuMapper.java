package com.zpx.rongyu.paixu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RongyuMapper extends Mapper<LongWritable, Text,RongyuBean  , NullWritable>
{

    private RongyuBean outK = new RongyuBean();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
    {
        String line = value.toString();

        String[] split = line.split("\t");

        String riqi = split[0];
        String ft = split[1];
        String lt = split[2];
        String wd = split[3];
        String shd = split[4];
        String num=split[5];

        this.outK.setDate(Long.parseLong(riqi));
        this.outK.setFirsttime(Long.parseLong(ft));
        this.outK.setLasttime(Long.parseLong(lt));
        this.outK.setTemper(Long.parseLong(wd));
        this.outK.setHumidity(Long.parseLong(shd));
        this.outK.setNumb(Long.parseLong(num));

        context.write(outK,NullWritable.get());


    }
}

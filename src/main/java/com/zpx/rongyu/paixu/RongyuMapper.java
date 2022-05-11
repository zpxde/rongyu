package com.zpx.rongyu.paixu;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RongyuMapper extends Mapper<LongWritable, Text, Text, RongyuBean>
{
    private Text outK = new Text();
    private RongyuBean outV = new RongyuBean();

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

        this.outK.set(riqi);
        this.outV.setFirsttime(Long.parseLong(ft));
        this.outV.setLasttime(Long.parseLong(lt));
        this.outV.setTemper(Long.parseLong(wd));
        this.outV.setHumidity(Long.parseLong(shd));

        context.write(this.outK, this.outV);
    }
}

package com.zpx.rongyu.find;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Scanner;

public class RongyuMapper extends Mapper<LongWritable, Text, Text, RongyuBean> {
    private Text outK = new Text();
    private RongyuBean outV = new RongyuBean();

    private  Scanner sc = new Scanner(System.in);

    String input = sc.next();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        //
        String line = value.toString();
        //2.切割
        String[] split = line.split("\t");
        //3.抓取想要的数据
        String riqi = split[0];
        String ft = split[1];
        String lt = split[2];
        String wd = split[3];
        String shd = split[4];
        //4.封装
        outK.set(riqi);
        outV.setFirsttime(Long.parseLong(ft));
        outV.setLasttime(Long.parseLong(lt));
        outV.setTemper(Long.parseLong(wd));
        outV.setHumidity(Long.parseLong(shd));
        //5.写出
        if (riqi.equals(input)) {
            context.write(outK, outV);
        }
    }
}

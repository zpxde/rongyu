package com.zpx.rongyu.basic;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RongyuDriver
{
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(RongyuDriver.class);

        job.setMapperClass(RongyuMapper.class);
        job.setReducerClass(RongyuReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RongyuBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RongyuBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\data\\yuanshishju"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output21"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
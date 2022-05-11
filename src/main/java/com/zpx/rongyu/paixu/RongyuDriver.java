package com.zpx.rongyu.paixu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

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

        job.setMapOutputKeyClass(RongyuBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(RongyuBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop\\data\\paixu"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output16"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
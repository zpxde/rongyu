package cpm.zpx.rongyu;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RongyuDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar
        job.setJarByClass(RongyuDriver.class);

        // 3 关联mapper 和Reducer
        job.setMapperClass(RongyuMapper.class);
        job.setReducerClass(RongyuReducer.class);

        // 4 设置mapper 输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RongyuBean.class);

        // 5 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RongyuBean.class);

        // 6 设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\shuju"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\output17"));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
}}

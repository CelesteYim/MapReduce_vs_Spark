package org.example;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class PageRank extends Configured implements Tool {
    // 设置全局配置项
    public static final int MAX_ITERATION = 20; // 最大迭代步数
    private static int iteration = 0; // 从0开始记录当前迭代步数
    public static final String TOTAL_PAGE = "1"; // 配置项中用于记录网页总数的键
    public static final String ITERATION = "2"; // 配置项中用于记录当前迭代步数的键

    @Override
    public int run(String[] args) throws  Exception {
//        System.out.println(args[0]);
//        System.out.println(args[1]);
//        System.out.println(args[2]);

        // 步骤1：设置作业的信息
//        int totalPage = Integer.parseInt(args[2]);
        int totalPage = 10;
        getConf().setInt(PageRank.ITERATION, iteration);
        getConf().setInt(PageRank.TOTAL_PAGE, totalPage);

        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        // 设置程序的类名
        job.setJarByClass(getClass());

        // 设置数据的输入路径
        if (iteration == 0) {
            FileInputFormat.addInputPath(job, new Path(args[1]));
        }
        else {
            // 将上一次迭代的输出设置为输入
            FileInputFormat.addInputPath(job, new Path(args[2] + (iteration - 1)));
        }
        // 设置数据的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[2] + iteration));

        // 设置Map方法及其输出键值对的数据类型
        job.setMapperClass(PageRankMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ReducePageRankWritable.class);

        // 设置Reduce方法及其输出键值对的数据类型
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        // 步骤2：运行作业，并计算运行时间
        int exitCode = 0;
        long startTime = System.currentTimeMillis();
        while (iteration < MAX_ITERATION) {
            exitCode = ToolRunner.run(new PageRank(), args);
            if (exitCode == -1) {
                break;
            }
            iteration++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println("programme runtime: " + (endTime - startTime) + "ms");
    }
}

/*
    MapReduce计算模型并不支持迭代，我们不可能通过一个MapReduce作业来完成整个迭代计算，而是需要使用一个MapReduce作业来实现单次迭代计算。
    计算某一网页p的排名值需要确定链向p的所有网页M(p)，并累加其中每一项网页p的排名值与出战链接数的比值PR(p)/L(p)，即贡献值。这
    一需求与Reduce任务的功能相吻合，只要Map任务产生以p的网页名称为键、PR(p)/L(p)为值的键值对，Reduce任务就能够收集到所有链向p的网页对
    其的贡献值，从而得到网页p的新排名值。
 */

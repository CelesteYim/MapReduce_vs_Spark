package org.example;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// 步骤1：确定输入键值对[K1, V1]的数据类型为[LongWritable, Text]，输出键值对[K2, V2]的数据类型为[Text, ReducePageRankWritable]
public class PageRankMapper extends Mapper<LongWritable , Text, Text, ReducePageRankWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 步骤2：编写处理逻辑，将[K1, V1]转换为[K2, V2]并输出
        // 以空格为分隔符切割
        String[] pageInfo = value.toString().split(" ");
//        System.out.println(pageInfo);

        // 网页的排名值
        double pageRank = Double.parseDouble(pageInfo[1]);
//        System.out.println(pageRank);

        // 网页的出站链接数
        int outLink = (pageInfo.length-2)/2;
//        System.out.println(outLink);

        ReducePageRankWritable writable;
        writable = new ReducePageRankWritable();
        // 计算贡献值并保存
        writable.setData(String.valueOf(pageRank / outLink));
        // 设置对应的标识
        writable.setTag(ReducePageRankWritable.PR_L); //贡献值

        // 对于每个出战链接，输出贡献值
        for (int i = 2; i < pageInfo.length; i += 2) {
//            System.out.println(pageInfo[i]);
            context.write(new Text(pageInfo[i]), writable);
        }
        writable = new ReducePageRankWritable();

        // 保存网页信息并标识
//        System.out.println(value.toString());
        writable.setData(value.toString());
        writable.setTag(ReducePageRankWritable.PAGE_INFO); //网页信息
        // 以输入的网页信息的网页名称为键进行输出
        context.write(new Text(pageInfo[0]), writable);
    }
}


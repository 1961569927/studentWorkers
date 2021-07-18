package com.student.works;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class MapperSplit extends Mapper<LongWritable, Text, Text, FlowBean> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        FlowBean outputvalue = new FlowBean();

        for (String word : words) {
            //手机号
            String phone = words[1];
            //上行流量
            Long upFlow = Long.valueOf(words[8]);
            //下行流量
            Long downFlow =  Long.valueOf(words[8]);
            //封装到FlowBean对象中
            outputvalue.set(upFlow,downFlow);
            //输出
            context.write(new Text(phone), outputvalue);
        }
    }

}

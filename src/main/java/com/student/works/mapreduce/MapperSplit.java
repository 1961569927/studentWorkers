package com.student.works.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class MapperSplit extends Mapper<LongWritable, Text, Text, FlowBean> {
    FlowBean output = new FlowBean();
    Text phone = new Text();
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        String[] words = value.toString().split("\t");
        phone.set(words[1]);
        //上行流量
        Long upFlow = Long.valueOf(words[8]);
        //下行流量
        Long downFlow =  Long.valueOf(words[9]);
        //封装到FlowBean对象中
        output.set(upFlow,downFlow);
        //输出
        context.write(phone, output);
    }

}

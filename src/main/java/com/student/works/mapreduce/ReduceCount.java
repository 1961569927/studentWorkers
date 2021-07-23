package com.student.works.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceCount extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;
        for (FlowBean value : values) {
            //上行流量求和
            sum_upFlow += value.getUpFlow();
            //下行流量求和
            sum_downFlow += value.getDownFlow();
        }
        //封装到最终的Bean中
        FlowBean fianlBean = new FlowBean(sum_upFlow, sum_downFlow);
        //输出
        context.write(key, fianlBean);
    }
}
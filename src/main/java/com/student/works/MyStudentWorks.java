package com.student.works;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyStudentWorks{
        public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException  {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(MyStudentWorks.class);

            job.setMapperClass(MapperSplit.class);
            job.setReducerClass(ReduceCount.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowBean.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

            FileInputFormat.setInputPaths(job, new Path("/user/student/diyukun/HTTP_20130313143750.dat"));
            FileOutputFormat.setOutputPath(job, new Path("/user/student/diyukun/output"));

            boolean b = job.waitForCompletion(true);
            System.exit(b ? 0 : 1);
        }
}

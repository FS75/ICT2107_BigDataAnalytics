package com.example.glassdoorsentiment;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class GlassDoorSentiment {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "GlassDoorSentiment");

		job.setJarByClass(GlassDoorSentiment.class);

		// One reducer to give a combined result
		job.setReducerClass(GlassDoorReducer.class);
		
		 // Set the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

		// Set the input format class to read multiple input files
		job.setInputFormatClass(TextInputFormat.class);

		// Set the input paths and mapper classes using MultipleInputs
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Amazon_glassdoor_review_cleaned.csv"),
                TextInputFormat.class, GlassDoorAmazonMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Ntuc_Fairprice_glassdoor_review_cleaned.csv"),
                TextInputFormat.class, GlassDoorNtucMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Sembcorp_glassdoor_review_cleaned.csv"),
                TextInputFormat.class, GlassDoorSembcorpMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/amazon_indeed_review_cleaned.csv"),
                TextInputFormat.class, IndeedAmazonMapper.class);
		
		
		// Set the output path
        Path outputPath = new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/output/"
            + new Date().getTime());
        FileOutputFormat.setOutputPath(job, outputPath);

		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}

package com.example.ratingcount;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class RatingCountAnalysis {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "RatingCount");

		job.setJarByClass(RatingCountAnalysis.class);

		// One reducer to give a combined result
		job.setReducerClass(RatingCountReducer.class);
		job.setMapperClass(RatingCountMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Add stopwords.txt as a cache file
		// edit the hdfs path here
		String hdfsPath = "hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/";
		Path stopwordPath = new Path(hdfsPath + "stopwords.txt");
		job.addCacheFile(stopwordPath.toUri());
		
		// add new files as required
		String[] fileNames = { "accenture_glassdoor_review_clean.csv", "accenture_indeed_review_clean.csv",
				"amazon_glassdoor_review_clean.csv", "amazon_indeed_review_clean.csv",
				"apple_glassdoor_review_clean.csv", "apple_indeed_review_clean.csv",
				"dbs-bank_glassdoor_review_clean.csv", "dbs-bank_indeed_review_clean.csv",
				"google_glassdoor_review_clean.csv", "google_indeed_review_clean.csv",
				"hsbc_glassdoor_review_clean.csv", "hsbc_indeed_review_clean.csv",
				"infineon-technologies_glassdoor_review_clean.csv", "infineon-technologies_indeed_review_clean.csv",
				"meta_glassdoor_review_clean.csv", "meta_indeed_review_clean.csv", "sembcorp_indeed_review_clean.csv",
				"st-engineering_glassdoor_review_clean.csv", "st-engineering_indeed_review_clean.csv",
				"united-overseas-bank_glassdoor_review_clean.csv", "united-overseas-bank_indeed_review_clean.csv",
				"micron-technology_glassdoor_review_clean.csv", "micron-technology_indeed_review_clean.csv",
				"netflix_glassdoor_review_clean.csv", "netflix_indeed_review_clean.csv",
				"ntuc-fairprice_glassdoor_review_clean.csv", "ntuc-fairprice_indeed_review_clean.csv",
				"sembcorp_glassdoor_review_clean.csv"};

		String inputPaths = "";
		for (int i = 0; i < fileNames.length; i++) {
			inputPaths += hdfsPath + fileNames[i];
			if (i < fileNames.length - 1) {
				inputPaths += ",";
			}
		}

		FileInputFormat.addInputPaths(job, inputPaths);

		// Set the output path
		Path outputPath = new Path(
				"hdfs://hadoop-master:9000/user/ict2101702/ratingCount/" + new Date().getTime());
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}

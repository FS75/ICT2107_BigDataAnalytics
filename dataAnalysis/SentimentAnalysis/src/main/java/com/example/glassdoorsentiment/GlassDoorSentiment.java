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
		//GlassDoor Mappers
		
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Amazon_glassdoor_review_clean.csv"),
                TextInputFormat.class, GlassDoorAmazonMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Ntuc_Fairprice_glassdoor_review_clean.csv"),
                TextInputFormat.class, GlassDoorNtucMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Sembcorp_glassdoor_review_clean.csv"),
                TextInputFormat.class, GlassDoorSembcorpMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/meta_glassdoor_review_clean.csv"),
                TextInputFormat.class, GlassDoorMetaMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/United_Overseas_Bank_glassdoor_review_clean.csv"),
                TextInputFormat.class, GlassDoorUOBMapper.class);
        
        //Indeed Mappers
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Amazon_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedAmazonMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Accenture_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedAccentureMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/apple_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedAppleMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Dbs-Bank_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedDBSMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/google_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedGoogleMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/HSBC_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedHSBCMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Infineon-Technologies_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedInfineonMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Meta-dd1502f2_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedMetaMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Micron-Technology-Inc_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedMicronMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Netflix_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedNetflixMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Ntuc-Fairprice_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedNtucMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/Sembcorp_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedSembcorpMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/St-Engineering_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedSTEngineeringMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/input/United-Overseas-Bank_indeed_review_clean.csv"),
                TextInputFormat.class, IndeedUOBMapper.class);
		
		
		// Set the output path
        Path outputPath = new Path("hdfs://hadoop-master:9000/user/ict2101702/glassDoor/output/"
            + new Date().getTime());
        FileOutputFormat.setOutputPath(job, outputPath);

		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}

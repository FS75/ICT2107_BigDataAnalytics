package com.example.ratingcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RatingCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private String company;
	private String platform;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String inputPath = ((FileSplit) context.getInputSplit()).getPath().toString();
		String[] inputPathParts = inputPath.split("/");
		
		company = inputPathParts[inputPathParts.length-1].split("_")[0];
		platform = inputPathParts[inputPathParts.length-1].split("_")[1].toLowerCase();
		super.setup(context);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    if (key.get() == 0) {
	        // skip header row
	        return;
	    }

	    // parse the CSV line
	    String[] fields = line.split(",", 9);

	    // get the text fields
	    String ratingFieldText = fields[0];

	    // split the text fields into words and count each word
	    try {
	        long rating = Long.parseLong(ratingFieldText);
	        Text outputKey = new Text(company + "|" + platform + "|Rating|");
	        LongWritable outputValue = new LongWritable(rating);
	        context.write(outputKey, outputValue);
	    } catch (NumberFormatException e) {
	        // ignore invalid ratings
	    }
	}
}

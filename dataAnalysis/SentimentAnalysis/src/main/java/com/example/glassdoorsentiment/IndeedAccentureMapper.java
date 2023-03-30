package com.example.glassdoorsentiment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class IndeedAccentureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private Map<String, Integer> afinnMap = new HashMap<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Load AFINN-111 word list from file on HDFS
		Configuration conf = context.getConfiguration();
		Path path = new Path("hdfs://hadoop-master:9000/user/ict2101702/AFINN-111.txt");
		FileSystem fs = path.getFileSystem(conf);
		try (FSDataInputStream inputStream = fs.open(path);
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("\t");
				String word = parts[0];
				int score = Integer.parseInt(parts[1]);
				afinnMap.put(word, score);
			}
		}
	}

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    if (key.get() == 0 && line.startsWith("Unnamed: 0")) {
	        // skip header row
	        return;
	    }

	    // parse the CSV line
	    String[] fields = line.split(",", 9);

	    // perform sentiment analysis using AFINN-111 word list
	    String proFieldText = fields[3];
	    double sentimentScore1 = performSentimentAnalysis(proFieldText, afinnMap);

	    String conFieldText = fields[4];
	    double sentimentScore2 = performSentimentAnalysis(conFieldText, afinnMap);

	    String descriptionFieldText = fields[2];
	    double sentimentScore3 = performSentimentAnalysis(descriptionFieldText, afinnMap);
	    // calculate the total sentiment score for the document
	    double totalSentimentScore = sentimentScore1 + sentimentScore2 + sentimentScore3;

	    // emit the total sentiment score for the company as a DoubleWritable value
	    String companyName = "AccentureIndeed";
	    DoubleWritable sentiment = new DoubleWritable(totalSentimentScore);
	    context.write(new Text(companyName), sentiment);
	}

	private double performSentimentAnalysis(String text, Map<String, Integer> afinnMap) {
		int totalScore = 0;
		int wordCount = 0;
		String[] words = text.split("\\s+");
		for (String word : words) {
			Integer score = afinnMap.get(word.toLowerCase());
			if (score != null) {
				totalScore += score;
				wordCount++;
			}
		}
		if (wordCount == 0) {
			return 0;
		}
		double averageScore = (double) totalScore / wordCount;
		return averageScore;
	}
}

package com.example.wordcount;

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

public class WordCountConsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable(1);
	private String company;
	private String platform;
	private Map<String, Long> wordCounts;
	ArrayList<String> stopwords = new ArrayList<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String inputPath = ((FileSplit) context.getInputSplit()).getPath().toString();
		String[] inputPathParts = inputPath.split("/");
		
		company = inputPathParts[inputPathParts.length-1].split("_")[0];
		platform = inputPathParts[inputPathParts.length-1].split("_")[1].toLowerCase();
		super.setup(context);
		wordCounts = new TreeMap<>();

		// We will put the ISO-3166-alpha3 tv to Distributed Cache in the driver class
		// so we can access to it here locally by its name
		// Read the stopwords file from the cache
		URI[] cacheFiles = context.getCacheFiles();
		Path stopwordsPath = new Path(cacheFiles[0]);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopwordsPath)))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        stopwords.add(line);
		    }
		}
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
//			String proFieldText = fields[3];
			String conFieldText = fields[4];

			// split the text fields into words and count each word
//			countWords(proFieldText, context);
			countWords(conFieldText, context);
		}

		private void countWords(String text, Context context) throws IOException, InterruptedException {
		    // Remove stopwords from the text
		    String filteredText = removeStopwords(text);

		    // Split the filtered text into words and count each word
		    String[] words = filteredText.split("\\s+");
		    for (String word : words) {
		        Text outputKey = new Text(company + "|" + platform + "|Cons|"+ word);
		        context.write(outputKey, new LongWritable(1));
		    }
		}
		
		private String removeStopwords(String input) {
			input = input.toLowerCase();
	        // Split the input string into words
	        String[] words = input.split("\\s+");

	        // Convert the stopwords ArrayList to an array
	        String[] stopwordsArray = stopwords.toArray(new String[0]);

	        // Create a string to store the non-stop words
	        String outputString = "";

	        // Loop through the words and add the non-stop words to the output string
	        for (String word : words) {
	            if (!Arrays.asList(stopwordsArray).contains(word) && !word.matches(".*\\d.*")) {
	                outputString += word + " ";
	            }
	        }

	        // Trim any extra whitespace from the output string
	        outputString = outputString.trim();

	        return outputString;
	    }

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);

			// Emit the word counts for each word
			for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
				Text outputKey = new Text(company + "|" + platform + "|Cons|" + entry.getKey());
				context.write(outputKey, new LongWritable(entry.getValue()));
			}
		}	
}

package com.example.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	private Map<String, TreeMap<Long, List<String>>> companyWordCounts;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        companyWordCounts = new HashMap<String, TreeMap<Long, List<String>>>();
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        // Sum up the values for each key
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }

        // Add the sum to the companyWordCounts map
        String[] parts = key.toString().split("\\|");
        String company = "";
        String word = "";
        String platform = "";
        try {
            company = parts[0];
            platform = parts[1];
            word = parts[3];
        } catch (ArrayIndexOutOfBoundsException e) {
            // Skip this key if it doesn't have the expected number of fields
            return;
        }
        
        String companyAndPlatform = company + "|" + platform;
        if (!companyWordCounts.containsKey(companyAndPlatform)) {
            companyWordCounts.put(companyAndPlatform, new TreeMap<Long, List<String>>(new Comparator<Long>() {
                public int compare(Long o1, Long o2) {
                    return o2.compareTo(o1);
                }
            }));
        }
        TreeMap<Long, List<String>> wordCounts = companyWordCounts.get(companyAndPlatform);
        if (!wordCounts.containsKey(sum)) {
            wordCounts.put(sum, new ArrayList<String>());
        }
        List<String> words = wordCounts.get(sum);
        words.add(word);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        // Emit the top 50 word counts for each company
        for (Map.Entry<String, TreeMap<Long, List<String>>> companyEntry : companyWordCounts.entrySet()) {
            String company = companyEntry.getKey();
            TreeMap<Long, List<String>> wordCounts = companyEntry.getValue();
            int count = 0;
            for (Map.Entry<Long, List<String>> wordEntry : wordCounts.entrySet()) {
                long wordCount = wordEntry.getKey();
                List<String> words = wordEntry.getValue();
                for (String word : words) {
                    Text outputKey = new Text(company + "|Cons|"+ word + "|");
                    context.write(outputKey, new LongWritable(wordCount));
                    count++;
                    if (count >= 50) {
                        break;
                    }
                }
                if (count >= 50) {
                    break;
                }
            }
        }
    }
}

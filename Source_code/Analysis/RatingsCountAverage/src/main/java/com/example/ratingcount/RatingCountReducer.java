package com.example.ratingcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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

public class RatingCountReducer extends Reducer<Text, LongWritable, Text, String> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        // Calculate the sum and count for the key
        long sum = 0;
        int count = 0;
        for (LongWritable value : values) {
            sum += value.get();
            count++;
        }

        // Extract the company and website from the key
        String[] parts = key.toString().split("\\|");
        String company = parts[0];
        String website = parts[1];

        // Emit the average rating for the company and website
        double averageRating = (double) sum / count;;
        String formattedSentiment = String.format("%.2f", averageRating);
        Text outputKey = new Text(company + "|" + website + "|");
        String outputValue = new String(formattedSentiment);
        context.write(outputKey, outputValue);
    }
}


package com.example.glassdoorsentiment;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GlassDoorReducer extends Reducer<Text, DoubleWritable, Text, CompanySentimentWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        // Sum up the sentiment scores for the company
        double companySentiment = 0;
        int count = 0;
        for (DoubleWritable value : values) {
            companySentiment += value.get();
            count++;
        }
        double averageSentiment = companySentiment / count;
        String formattedSentiment = String.format("%.2f", averageSentiment);

        // Write the sentiment score for the company to the output
        CompanySentimentWritable outputValue = new CompanySentimentWritable(key.toString(), formattedSentiment);
        context.write(null, outputValue);
    }
}





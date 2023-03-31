package com.example.glassdoorsentiment;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CompanySentimentWritable implements Writable {
    private String companyName;
    private String sentimentScore;

    public CompanySentimentWritable() {}

    public CompanySentimentWritable(String companyName, String sentimentScore) {
        this.companyName = companyName;
        this.sentimentScore = sentimentScore;
    }

    public String getCompanyName() {
        return companyName;
    }

    public String getSentimentScore() {
        return sentimentScore;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(companyName);
        out.writeUTF(sentimentScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        companyName = in.readUTF();
        sentimentScore = in.readUTF();
    }

    @Override
    public String toString() {
        return companyName + ": " + sentimentScore;
    }
}

package sentimentanalysis;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
        return companyName + sentimentScore;
    }
}
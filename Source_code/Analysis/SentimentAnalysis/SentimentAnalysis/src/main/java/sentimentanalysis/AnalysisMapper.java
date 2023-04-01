package sentimentanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AnalysisMapper extends Mapper<Text, ProAndConWritable, Text, DoubleWritable> {
    private Map<String, Integer> afinnMap = new HashMap<>();
    private String proField, conField, company, platform;

    @Override
    protected void setup(Context context) throws IOException {

        String inputPath = ((FileSplit) context.getInputSplit()).getPath().toString();
        String[] inputPathParts = inputPath.split("/");

        company = inputPathParts[inputPathParts.length-1].split("_")[0];
        platform = inputPathParts[inputPathParts.length-1].split("_")[1].toLowerCase();

        // Load AFINN-111 word list from file on HDFS
        Configuration conf = context.getConfiguration();

        //		Path path = new Path("hdfs://hadoop-master:9000/user/ict2100868/project/input/AFINN-111.txt");
        //		FileSystem fs = path.getFileSystem(conf);
        //		try (FSDataInputStream inputStream = fs.open(path);
        BufferedReader reader = new BufferedReader(new FileReader("AFINN-111.txt"));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            String word = parts[0];
            int score = Integer.parseInt(parts[1]);
            afinnMap.put(word, score);
        }
    }

    protected void map(Text key, ProAndConWritable value, Context context) throws IOException, InterruptedException {

        proField = value.getPro();
        conField = value.getCon();

        String descriptionField = key.toString();
        double sentimentScore1 = performSentimentAnalysis(descriptionField, afinnMap);

        double sentimentScore2 = performSentimentAnalysis(proField, afinnMap);

        double sentimentScore3 = performSentimentAnalysis(conField, afinnMap);

        // calculate the total sentiment score for the document
        double totalSentimentScore = sentimentScore1 + sentimentScore2 + sentimentScore3;

        // emit the total sentiment score for the company as a DoubleWritable value
        String companyName = company + "|" + platform + "|";
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

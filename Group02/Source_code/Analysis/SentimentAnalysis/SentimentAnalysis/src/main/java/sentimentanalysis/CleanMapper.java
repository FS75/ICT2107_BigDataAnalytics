package sentimentanalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class CleanMapper extends Mapper<LongWritable, Text, Text, ProAndConWritable> {
    ArrayList<String> stopwords = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"));
        String line = null;

        while (true) {
            line = br.readLine();
            if (line != null) {
                stopwords.add(line);
            } else {
                break;// finished reading
            }
        }

        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (key.get() == 0 && line.startsWith("Unnamed: 0")) {
            // skip header row
            return;
        }

        // parse the CSV line
        String[] fields = line.split(",", 9);

        String descriptionFieldText = fields[2];
        String proFieldText = fields[3];
        String conFieldText = fields[4];

        String cleanedDescription = removeStopwords(descriptionFieldText);
        String cleanedPro = removeStopwords(proFieldText);
        String cleanedCon = removeStopwords(conFieldText);

//        proAndConWritable.put(new Text(cleanedPro), new Text(cleanedCon));
        ProAndConWritable proAndConWritable = new ProAndConWritable(cleanedPro, cleanedCon);

        // emit the total sentiment score for the company as a DoubleWritable value
        context.write(new Text(cleanedDescription), proAndConWritable);

    }
    private String removeStopwords(String input) {
        // Split the input string into words
        String[] words = input.split("\\s+");

        // Convert the stopwords ArrayList to an array
        String[] stopwordsArray = stopwords.toArray(new String[0]);

        // Create a string to store the non-stop words
        String outputString = "";

        // Loop through the words and add the non-stop words to the output string
        for (String word : words) {
            if (!Arrays.asList(stopwordsArray).contains(word)) {
                outputString += word + " ";
            }
        }

        // Trim any extra whitespace from the output string
        outputString = outputString.trim();

        return outputString;
    }
}
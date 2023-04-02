package industrysentiment;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IndustrySentimentReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Double> sentimentSums = new HashMap<>();
    private Map<String, Integer> sentimentCounts = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String industry = null;
        Double sentiment = 0.0;

        for (Text t : values) {
            String parts[] = t.toString().split("\t");

            if (parts[0].equals("sentiment")) {
                sentiment = Double.parseDouble(parts[1]);
            } else if (parts[0].equals("company-industry")) {
                industry = parts[1];
            }
        }

        if (industry != null && sentiment != null) {
            if (sentimentSums.containsKey(industry)) {
                sentimentSums.put(industry, sentimentSums.get(industry) + sentiment);
            } else {
                sentimentSums.put(industry, sentiment);
            }

            if (sentimentCounts.containsKey(industry)) {
                sentimentCounts.put(industry, sentimentCounts.get(industry) + 1);
            } else {
                sentimentCounts.put(industry, 1);
            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        DecimalFormat sentimentDF = new DecimalFormat("#.##");
        for (Map.Entry<String, Double> entry : sentimentSums.entrySet()) {
            String industry = entry.getKey();
            double averageSentiment = entry.getValue() / sentimentCounts.get(industry);
            context.write(null, new Text(industry + "|" + sentimentDF.format(averageSentiment)));
        }
    }

}

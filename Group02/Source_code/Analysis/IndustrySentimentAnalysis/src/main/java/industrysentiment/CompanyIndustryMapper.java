package industrysentiment;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CompanyIndustryMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (key.get() == 0) {
            // skip header row
            return;
        }

        String[] parts = line.split(",");
        String companyName = parts[0];
        String industry = parts[1];

        context.write(new Text(companyName), new Text("company-industry\t" + industry));
    }
}

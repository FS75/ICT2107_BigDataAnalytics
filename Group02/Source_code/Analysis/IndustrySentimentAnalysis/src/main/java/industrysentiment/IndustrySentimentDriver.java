package industrysentiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;
import java.util.Date;


public class IndustrySentimentDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IndustrySentiment");
        job.setJarByClass(IndustrySentimentDriver.class);

        // Set the input format class to read multiple input files
        job.setInputFormatClass(TextInputFormat.class);

        Path sentimentsInputPath = new Path("hdfs://hadoop-master:9000/user/ict2100868/project/input/Sentiments.txt");
        Path companyIndustryPath = new Path("hdfs://hadoop-master:9000/user/ict2100868/project/input/company-industry.txt");
        Path outPath = new Path("hdfs://hadoop-master:9000/user/ict2100868/project/output/");
        outPath.getFileSystem(conf).delete(outPath, true);

        // Set the output path
        Path outputPath = new Path("hdfs://hadoop-master:9000/user/ict2100868/project/output/" + new Date().getTime());
        FileOutputFormat.setOutputPath(job, outputPath);

        MultipleInputs.addInputPath(job, sentimentsInputPath, TextInputFormat.class, IndustrySentimentMapper.class);
        MultipleInputs.addInputPath(job, companyIndustryPath, TextInputFormat.class, CompanyIndustryMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // One reducer to give a combined result
        job.setReducerClass(IndustrySentimentReducer.class);

        // Set the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit((job.waitForCompletion(true)) ? 0 : 1);
    }
}
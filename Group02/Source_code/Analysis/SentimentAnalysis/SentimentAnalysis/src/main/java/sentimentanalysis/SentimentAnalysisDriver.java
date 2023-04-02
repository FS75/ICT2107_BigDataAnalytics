package sentimentanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;
import java.util.Date;


public class SentimentAnalysisDriver {

    public static void main(String[] args) throws Exception {
        String hdfsPath = "hdfs://hadoop-master:9000/user/ict2100868/project/input/";
        String[] inputPathsArray = {
                hdfsPath + "accenture_glassdoor_review_clean.csv",
                hdfsPath + "accenture_indeed_review_clean.csv",
                hdfsPath + "amazon_glassdoor_review_clean.csv",
                hdfsPath + "amazon_indeed_review_clean.csv",
                hdfsPath + "apple_glassdoor_review_clean.csv",
                hdfsPath + "apple_indeed_review_clean.csv",
                hdfsPath + "dbs-bank_glassdoor_review_clean.csv",
                hdfsPath + "dbs-bank_indeed_review_clean.csv",
                hdfsPath + "google_glassdoor_review_clean.csv",
                hdfsPath + "google_indeed_review_clean.csv",
                hdfsPath + "hsbc_glassdoor_review_clean.csv",
                hdfsPath + "hsbc_indeed_review_clean.csv",
                hdfsPath + "infineon-technologies_glassdoor_review_clean.csv",
                hdfsPath + "infineon-technologies_indeed_review_clean.csv",
                hdfsPath + "meta_glassdoor_review_clean.csv",
                hdfsPath + "meta_indeed_review_clean.csv",
                hdfsPath + "sembcorp_indeed_review_clean.csv",
                hdfsPath + "st-engineering_glassdoor_review_clean.csv",
                hdfsPath + "st-engineering_indeed_review_clean.csv",
                hdfsPath + "united-overseas-bank_glassdoor_review_clean.csv",
                hdfsPath + "united-overseas-bank_indeed_review_clean.csv",
                hdfsPath + "micron-technology_glassdoor_review_clean.csv",
                hdfsPath + "micron-technology_indeed_review_clean.csv",
                hdfsPath + "netflix_glassdoor_review_clean.csv",
                hdfsPath + "netflix_indeed_review_clean.csv",
                hdfsPath + "ntuc-fairprice_glassdoor_review_clean.csv",
                hdfsPath + "ntuc-fairprice_indeed_review_clean.csv",
                hdfsPath + "sembcorp_glassdoor_review_clean.csv"
        };

        StringBuilder inputPaths = new StringBuilder();
        for (int i = 0; i < inputPathsArray.length; i++) {
            inputPaths.append(inputPathsArray[i]);
            if (i < inputPathsArray.length - 1) {
                inputPaths.append(",");
            }
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SentimentAnalysis");
        job.setJarByClass(SentimentAnalysisDriver.class);

        // Set the input format class to read multiple input files
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPaths(job, inputPaths.toString());

//        Path inPath = new Path("hdfs://hadoop-master:9000/user/ict2100868/project/input/");
        Path outPath = new Path("hdfs://hadoop-master:9000/user/ict2100868/project/output/");
        outPath.getFileSystem(conf).delete(outPath, true);

        // Set the output path
        Path outputPath = new Path("hdfs://hadoop-master:9000/user/ict2100868/project/output/" + new Date().getTime());
        FileOutputFormat.setOutputPath(job, outputPath);

        job.addCacheFile(new URI("hdfs://hadoop-master:9000/user/ict2100868/project/input/AFINN-111.txt"));
        job.addCacheFile(new URI("hdfs://hadoop-master:9000/user/ict2100868/project/input/stopwords.txt"));

        Configuration cleanConf = new Configuration(false);
        ChainMapper.addMapper(job, CleanMapper.class, LongWritable.class, Text.class, Text.class, ProAndConWritable.class, cleanConf);

        Configuration amazonGlassDoorConf = new Configuration(false);
        ChainMapper.addMapper(job, AnalysisMapper.class, Text.class, ProAndConWritable.class, Text.class, DoubleWritable.class, amazonGlassDoorConf);

        job.setMapperClass(ChainMapper.class);

        // One reducer to give a combined result
        job.setReducerClass(AnalysisReducer.class);

        // Set the output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit((job.waitForCompletion(true)) ? 0 : 1);
    }
}

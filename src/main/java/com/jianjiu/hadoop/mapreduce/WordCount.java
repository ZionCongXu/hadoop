package com.jianjiu.hadoop.mapreduce;

import java.io.IOException;

import com.jianjiu.hadoop.mapreduce.mapper.WordCountMapper;
import com.jianjiu.hadoop.mapreduce.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author jianjiu.xc
 * @version : WordCount, v 0.1 2020-11-13 10:06 jianjiu.xc Exp $
 */
public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        System.out.println("[DEBUG] " + inputPath.toUri().getPath());
        System.out.println("[DEBUG] " + outputPath.toUri().getPath());
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

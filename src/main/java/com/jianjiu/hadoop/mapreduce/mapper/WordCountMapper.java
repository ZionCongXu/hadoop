package com.jianjiu.hadoop.mapreduce.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;


/**
 * @author jianjiu.xc
 * @version : WordCountMapper, v 0.1 2020-11-13 10:11 jianjiu.xc Exp $
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable intWritable = new IntWritable(1);
    private Text word = new Text();

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println("[DEBUG] WordCountMapper key = " + key.toString());
        System.out.println("[DEBUG] WordCountMapper value = " + value.toString());
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while (stringTokenizer.hasMoreTokens()){
            word.set(stringTokenizer.nextToken());
            context.write(word, intWritable);
        }
    }
}

package com.jianjiu.hadoop.mapreduce.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author jianjiu.xc
 * @version : WordCountReducer, v 0.1 2020-11-13 10:11 jianjiu.xc Exp $
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        System.out.println("[DEBUG] WordCountReducer key = " + key.toString());
        List<Integer> intWritableList = new ArrayList<Integer>();

        int sum = 0;
        for(IntWritable intWritable : values){
            intWritableList.add(intWritable.get());
            sum += intWritable.get();
        }

        System.out.println("[DEBUG] WordCountReducer values = " + JSON.toJSONString(intWritableList));

        result.set(sum);
        context.write(key, result);
    }
}

package tn.isima.exercice;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NumMaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

boolean foundMax = false;

public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        if(!foundMax){
            for(IntWritable t : values){
                context.write(new Text(key),t);
            }
            foundMax = true;
        }              
}
}
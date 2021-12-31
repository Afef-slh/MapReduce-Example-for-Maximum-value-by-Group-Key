package tn.isima.exercice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NumMaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {



public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
    int max = 0;
    for(IntWritable x : values){
        if(max < Integer.parseInt(String.valueOf(x))){
            max = Integer.parseInt(String.valueOf(x));
        }
    }
    context.write(key, new IntWritable(max));
}
}

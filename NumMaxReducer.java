package tn.isima.exercice;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NumMaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	ArrayList<Integer> List = new ArrayList<Integer>();


public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
    int max = 0;
   
    for(IntWritable x : values){
        max=Math.max(max, x.get());
    }
    System.out.println("Nombre de Share="+max );
    context.write(key,new IntWritable(max));
}
}

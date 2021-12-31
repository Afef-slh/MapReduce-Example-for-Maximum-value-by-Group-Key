package tn.isima.exercice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NumMaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private Text outKey= new Text();


public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
    int max = 0;
    String keyWithMax="";
    for(IntWritable x : values){
        if(x.get()> max){
            max = x.get();
            outKey.set(keyWithMax);
        }
    }
    context.write(outKey, new IntWritable(max));
}
}

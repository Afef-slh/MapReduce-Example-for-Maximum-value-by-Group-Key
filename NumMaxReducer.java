package tn.isima.exercice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class NumMaxReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

private IntWritable result = new IntWritable();

public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException  {


int max=0;


for (IntWritable result : values) {
if (result == null || result.get() > max) {
	max=result.get();

}
result.set(max);
context.write(key, result);
}
}
}
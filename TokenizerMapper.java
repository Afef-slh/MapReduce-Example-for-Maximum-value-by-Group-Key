package tn.isima.exercice;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class TokenizerMapper
extends Mapper<LongWritable, Text, Text, IntWritable>{
	int max = Integer.MIN_VALUE;
	String token;

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] tokens = value.toString().split(",");
	          
	            int val = Integer.parseInt(tokens[9]);
	            if(Integer.parseInt(tokens[9]) > max){
	                max = val;
	                token = tokens[0];
	            }
	        }
	

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {    
	    context.write(new Text(token), new IntWritable(max));    
	}
	
		
	

}

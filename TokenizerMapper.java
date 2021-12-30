package tn.isima.exercice;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public  class TokenizerMapper
extends Mapper<Object, Text, Text, IntWritable>{


private Integer maxnum=0;
private String author="";


private IntWritable outPut= new IntWritable();

public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {

	String[] tweets= value.toString().split(",",-1);
	//10001,'telephone','may','mon',100,1,999,0,'nonexistent','no',0
	
	
	
	if (null != tweets &&  tweets[9].length() >0) {
		maxnum=Integer.parseInt(tweets[9]);
		context.write(new Text(author), new IntWritable(maxnum));
	}
	System.out.println("author="+author+"Nombre de Share="+ maxnum);
	
		

}
}
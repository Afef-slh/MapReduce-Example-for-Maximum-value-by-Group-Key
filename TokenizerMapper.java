package tn.isima.exercice;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public  class TokenizerMapper
extends Mapper<Object, Text, Text, IntWritable>{


private Integer maxnum=0;





public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
	String line = value.toString();

	String[] data=line.split("\t");
	//10001,'telephone','may','mon',100,1,999,0,'nonexistent','no',0
	
	
	
	if (null != data &&  data[9].length() >0) {
		maxnum=Integer.parseInt(data[9]);
		context.write(new Text(data[0]), new IntWritable(maxnum));
	}
	System.out.println("author="+data[0]+"Nombre de Share="+ maxnum);
	
		

}
}
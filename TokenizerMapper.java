package tn.isima.exercice;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TokenizerMapper extends Mapper<Object, Text, Text, CustomMaxTuple> {
private CustomMaxTuple outTuple = new CustomMaxTuple();
private Text departmentName = new Text();
@Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
String data = value.toString();
String[] field = data.split(",", -1);
double share = 0;
if (null != field  && field[9].length() >0) {
share=Double.parseDouble(field[9]);

outTuple.setMax(share);

departmentName.set(field[0]);
context.write(departmentName, outTuple);
}
}
}

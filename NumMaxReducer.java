package tn.isima.exercice;

import java.io.IOException;



import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NumMaxReducer extends Reducer<Text, CustomMaxTuple, Text, CustomMaxTuple> {
private CustomMaxTuple result = new CustomMaxTuple();
public void reduce(Text key, Iterable<CustomMaxTuple> values, Context context)
throws IOException, InterruptedException {

result.setMax(null);


for (CustomMaxTuple minMaxCountTuple : values) {
if (result.getMax() == null || (minMaxCountTuple.getMax() > result.getMax())) {
result.setMax(minMaxCountTuple.getMax());
}


context.write(new Text(key.toString()), result);
}
}
}
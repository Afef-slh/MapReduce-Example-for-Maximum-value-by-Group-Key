package tn.isima.exercice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
public class CustomMaxTuple implements Writable {

private double max = 0;



public Double getMax() {
return max;
}
public void setMax(Double max) {
this.max = max;
}

public void readFields(DataInput in) throws IOException {
max = in.readDouble();

}
public void write(DataOutput out) throws IOException {

out.writeDouble(max);

}
public String toString() {
return max + "\t" ;
}
}

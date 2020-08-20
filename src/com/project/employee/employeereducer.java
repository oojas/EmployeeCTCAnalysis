package com.project.employee;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;

public class employeereducer extends Reducer<Text , Text, NullWritable, Text>{
	
	 public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
		 int maxctc=Integer.MIN_VALUE;
		 int ctc=0;
		 String value="";
		 for(Text val : values) {
			 String tokens[]=val.toString().split("\\t");
			ctc=Integer.parseInt(tokens[5]);
			if(ctc>maxctc) {
				maxctc=ctc;
				value=val.toString();
			}
		 }
		 context.write(NullWritable.get(), new Text(value));
	 }
}

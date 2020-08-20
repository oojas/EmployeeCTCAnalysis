package com.project.employee;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;
import java.io.*;
public class employeeMapper extends Mapper<Object,Text,Text,Text> {
	public void map(Object key,Text value,Context context)throws IOException, InterruptedException{
		String tokens[]=value.toString().split("\\t");
		if(tokens.length==6) {
			String dept=tokens[4];
		context.write(new Text(dept), new Text(value));
		}
	}
}

package com.project.employee;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class employeedriver {

	public static void main(String[] args)throws IOException, InterruptedException, ClassNotFoundException {
	Configuration conf=new Configuration();
	Job job=new Job(conf,"Employees CTC Analysis");
	job.setJobName("Partitioner Class");
	job.setJarByClass(employeedriver.class);
	job.setMapperClass(employeeMapper.class);
	job.setPartitionerClass(Employeepartitioner.class);
	job.setReducerClass(employeereducer.class);
	
	
	
	job.setNumReduceTasks(2);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
	
	
	FileInputFormat.addInputPath(job,new Path("/home/ojas/Data/employeedata.txt"));
	FileOutputFormat.setOutputPath(job,new Path("/home/ojas/output/employee-data-output8"));
	System.exit(job.waitForCompletion(true)?0 : 1);

	}

}

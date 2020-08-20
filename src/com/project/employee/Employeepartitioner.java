package com.project.employee;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;
public class Employeepartitioner extends Partitioner<Text,Text>{

	@Override
	public int getPartition(Text key, Text value, int numReducedTasks) {
		int partno=0;
		String tokens[]=value.toString().split("\\t");
		String gender =tokens[3];
		if(numReducedTasks!=0) {
			if(gender.equals("female")) {
				partno=0;
			}else {
				partno=1;
			}
		}
		
		return partno;
	}

}

package com.patientanalytics.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PatientMaritalStatusCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private enum Counter 
	{
		UNKNOWN
	}
	
	@Override
	public void map(LongWritable offset, Text text, Context context) throws InterruptedException, IOException {
	
		String line = text.toString();
		String [] lineArr = line.split("\t");
		String maritalStatus = lineArr[4];
		
		if (maritalStatus.equalsIgnoreCase("Unknown")){
			context.getCounter(Counter.UNKNOWN).increment(1l);
		}
		else{
			context.write(new Text(maritalStatus), new IntWritable(1));
		}
	}
}

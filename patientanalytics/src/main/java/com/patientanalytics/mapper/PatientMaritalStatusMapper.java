package com.patientanalytics.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.patientanalytics.pair.TextPair;

public class PatientMaritalStatusMapper extends Mapper<LongWritable, Text, TextPair, Text>{

	@Override
	public void map(LongWritable offset, Text text, Context context) throws InterruptedException, IOException {
	
		String line = text.toString();
		String [] lineArr = line.split("\t");
		String maritalStatus = lineArr[4];
		String gender = lineArr[1];
		
		context.write(new TextPair(new Text(gender), new Text(maritalStatus)), text);
	}
}

package com.patientanalytics.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AdmissionMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable offset, Text text, Context context) throws InterruptedException, IOException {
	
		String line = text.toString();
		String[] lineArr = line.split("\t");
		context.write(new Text(lineArr[0]), new Text("1"));
	}
}
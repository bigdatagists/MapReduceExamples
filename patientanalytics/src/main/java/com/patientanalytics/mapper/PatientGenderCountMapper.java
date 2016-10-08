package com.patientanalytics.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PatientGenderCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable offset, Text text, Context context) throws InterruptedException, IOException {
	
		String line = text.toString();
		StringTokenizer tokens = new StringTokenizer(line,"\t");
		while(tokens.hasMoreTokens())
		{	
			String token = tokens.nextToken();
			if(token.equalsIgnoreCase("Male")){
				context.write(new Text("Male"), new IntWritable(1));
			}
			else if(token.equalsIgnoreCase("Female")){
				context.write(new Text("Female"), new IntWritable(1));
			}
		}
	}
}

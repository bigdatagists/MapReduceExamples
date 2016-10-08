package com.patientanalytics.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PatientGenderCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text gender, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
		int totalCount = 0;

		for (IntWritable count : counts){
			totalCount = totalCount + Integer.parseInt(count.toString());
		}
		
		context.write(gender, new IntWritable(totalCount));
	}
}
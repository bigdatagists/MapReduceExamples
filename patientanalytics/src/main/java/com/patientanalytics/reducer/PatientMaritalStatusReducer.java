package com.patientanalytics.reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.patientanalytics.pair.TextPair;

public class PatientMaritalStatusReducer extends Reducer<TextPair, Text, Text, NullWritable>{

	@Override
	public void reduce(TextPair genderMaritalStatus, Iterable<Text> texts, Context context) throws InterruptedException, IOException{
		
		for (Text text : texts){
			context.write(text, NullWritable.get());
		}
	}
}

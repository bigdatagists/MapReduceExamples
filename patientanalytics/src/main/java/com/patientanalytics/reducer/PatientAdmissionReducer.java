package com.patientanalytics.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PatientAdmissionReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text patientId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for (Text value : values){
			if (!value.toString().startsWith(patientId.toString().trim())){
				count = count + Integer.parseInt(value.toString());
			}
		}
		context.write(patientId, new Text(Integer.toString(count)));
	}
}
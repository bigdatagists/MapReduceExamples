package com.patientanalytics.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.patientanalytics.mapper.PatientGenderCountMapper;
import com.patientanalytics.reducer.PatientGenderCountReducer;

public class PatientGenderCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Patient Gender Count");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PatientGenderCountMapper.class);
		job.setCombinerClass(PatientGenderCountReducer.class);
		job.setReducerClass(PatientGenderCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PatientGenderCountDriver(), args);
		System.exit(exitCode);
	}
}
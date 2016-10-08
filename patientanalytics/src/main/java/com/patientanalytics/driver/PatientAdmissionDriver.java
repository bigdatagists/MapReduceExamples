package com.patientanalytics.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.patientanalytics.mapper.AdmissionMapper;
import com.patientanalytics.mapper.PatientIdMapper;
import com.patientanalytics.reducer.PatientAdmissionReducer;

public class PatientAdmissionDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Patient Marital Count");
		job.setJarByClass(getClass());
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PatientIdMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AdmissionMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(PatientAdmissionReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PatientAdmissionDriver(), args);
		System.exit(exitCode);
	}
}

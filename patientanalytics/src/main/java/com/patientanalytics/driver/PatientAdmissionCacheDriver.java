package com.patientanalytics.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.patientanalytics.mapper.AdmissionCacheMapper;

public class PatientAdmissionCacheDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Patient Admission Count");
		job.setJarByClass(getClass());
		
		job.addCacheFile(new Path(args[0]).toUri());
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(AdmissionCacheMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PatientAdmissionCacheDriver(), args);
		System.exit(exitCode);
	}
}

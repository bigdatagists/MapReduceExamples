package com.patientanalytics.driver;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.patientanalytics.comparator.PatientGenderComparator;
import com.patientanalytics.comparator.PatientMaritalStatusComparator;
import com.patientanalytics.mapper.PatientMaritalStatusMapper;
import com.patientanalytics.pair.TextPair;
import com.patientanalytics.partitioner.PatientGenderPartitioner;
import com.patientanalytics.reducer.PatientMaritalStatusReducer;

public class PatientMaritalStatusSortDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Patient Language Sort");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PatientMaritalStatusMapper.class);
		job.setPartitionerClass(PatientGenderPartitioner.class);
		job.setSortComparatorClass(PatientMaritalStatusComparator.class);
		job.setGroupingComparatorClass(PatientGenderComparator.class);
		job.setReducerClass(PatientMaritalStatusReducer.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PatientMaritalStatusSortDriver(), args);
		System.exit(exitCode);
	}
}
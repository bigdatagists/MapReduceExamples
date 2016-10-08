package com.patientanalytics.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.patientanalytics.pair.TextPair;


public class PatientGenderPartitioner extends Partitioner<TextPair,Text>
{
	@Override
	public int getPartition(TextPair key, Text value, int numPartitions) {
		Text gender = key.getNaturalKey();
		return Math.abs(gender.hashCode() * 127) % numPartitions;
	}
}
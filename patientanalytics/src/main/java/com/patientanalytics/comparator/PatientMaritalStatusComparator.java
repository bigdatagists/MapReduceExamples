package com.patientanalytics.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.patientanalytics.pair.TextPair;

public class PatientMaritalStatusComparator extends WritableComparator {

	protected PatientMaritalStatusComparator() {
		super(TextPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		if (null == w1 || null == w2)
			return 1;

		TextPair ip1 = (TextPair) w1;
		TextPair ip2 = (TextPair) w2;
		return ip1.compareTo(ip2);
	}
}

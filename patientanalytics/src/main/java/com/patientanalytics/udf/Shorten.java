package com.patientanalytics.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Shorten extends UDF {

	private enum MaritalStatus {
		SINGLE("SI"), MARRIED("M"), DIVORCED("D"), SEPARATED("SE"), UNKNOWN("U"), PATIENTMARITALSTATUS("U"), WIDOWED("W");
		private String shortForm;
		private MaritalStatus(String shortForm) {
			this.shortForm = shortForm;
		}
	}

	public Text evaluate(Text maritalStatus) {
		try {
			if (maritalStatus == null) {
				return null;
			}
			return new Text(MaritalStatus.valueOf(maritalStatus.toString().toUpperCase()).shortForm);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}

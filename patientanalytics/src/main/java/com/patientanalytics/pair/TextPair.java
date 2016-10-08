package com.patientanalytics.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

	private Text naturalKey;
	private Text naturalValue;
	
	public TextPair(){
		this.naturalKey = new Text();
		this.naturalValue = new Text();
	}
	
	public TextPair(Text naturalKey, Text naturalValue)
	{
		this.naturalKey = naturalKey;
		this.naturalValue = naturalValue;
	}
	
	public Text getNaturalKey() {
		return naturalKey;
	}

	public Text getNaturalValue() {
		return naturalValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		naturalKey.readFields(in);
		naturalValue.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		naturalKey.write(out);
		naturalValue.write(out);		
	}

	@Override
	public int compareTo(TextPair o) {
		int cmp = this.naturalKey.toString().compareToIgnoreCase(o.getNaturalKey().toString());
		if (cmp != 0)
			return cmp;
		return this.naturalValue.toString().compareTo(o.naturalValue.toString());
	}
}
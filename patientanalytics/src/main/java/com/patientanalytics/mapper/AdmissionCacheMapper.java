package com.patientanalytics.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AdmissionCacheMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private List<String> patientCoreList = null;
	private Map<String, Integer> admissionCountMap = null;

	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		URI[] uris = context.getCacheFiles();
		Path path = new Path(uris[0].getPath());
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		if (fileSystem.exists(path)){
			patientCoreList = new ArrayList<String>();
			admissionCountMap = new HashMap<String, Integer>();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				String[] lineArr = line.split("\t");
				patientCoreList.add(lineArr[0]);
				context.getCounter("admissionCache", "patientCount").increment(1L);
			}
		}
	}

	@Override
	public void map(LongWritable offset, Text text, Context context) throws InterruptedException, IOException {
		if (null != patientCoreList) {
			String line = text.toString();
			String patientId = line.split("\t")[0];
			int count = patientCoreList.contains(patientId) ? admissionCountMap
					.containsKey(patientId) ? admissionCountMap.get(patientId) + 1
					: 1
					: -1;
			admissionCountMap.put(patientId, count);
		}
		context.getCounter("admissionCache", "admissionCountMap").setValue(admissionCountMap.size());
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		Iterator<Map.Entry<String, Integer>> it = admissionCountMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> entry = it.next();
			context.write(new Text(entry.getKey()),	new IntWritable(entry.getValue()));
		}
	}
}
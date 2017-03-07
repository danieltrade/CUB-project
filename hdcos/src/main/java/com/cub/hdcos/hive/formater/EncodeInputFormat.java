package com.cub.hdcos.hive.formater;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class EncodeInputFormat extends TextInputFormat implements JobConfigurable {
	
	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		reporter.setStatus(genericSplit.toString());
		return new EncodeRecordReader(job, (FileSplit) genericSplit);
	}
	
}

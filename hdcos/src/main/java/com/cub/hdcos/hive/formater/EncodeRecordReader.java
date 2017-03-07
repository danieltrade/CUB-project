package com.cub.hdcos.hive.formater;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

public class EncodeRecordReader implements RecordReader<LongWritable, Text> {
	
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader lineReader;
	int maxLineLength;
	
	LongWritable lineKey;
	Text lineValue;
	
	public EncodeRecordReader(InputStream in, long offset, long endOffset,int maxLineLength) {
		this.maxLineLength = maxLineLength;
		this.start = offset;
		this.lineReader = new LineReader(in);
		this.pos = offset;
		this.end = endOffset;
	}
	
	public EncodeRecordReader(JobConf job ,FileSplit inputSplit) {
		boolean skipFirstLine = false;
		maxLineLength = job.getInt("mapred.mutilCharRecordReader.maxlength",Integer.MAX_VALUE);
		start = inputSplit.getStart();
		end = start + inputSplit.getLength();
		final Path file = inputSplit.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		
		try {
			final CompressionCodec codec = compressionCodecs.getCodec(file);
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(file);
			
			if (codec != null) {
				lineReader = new LineReader(codec.createInputStream(fileIn), job);
				end = Long.MAX_VALUE;
			} else {
				if (start != 0) {
					skipFirstLine = true;
					--start;
					fileIn.seek(start);
				}
				lineReader = new LineReader(fileIn, job);
			}
			if (skipFirstLine) {
				start += lineReader.readLine(new Text(), 0,(int) Math.min((long) Integer.MAX_VALUE, end - start));
			}
			this.pos = start;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		while (pos < end) {
			key.set(pos);
			int newSize = lineReader.readLine(value, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),maxLineLength));
			// 把字符串中的"$#$"转变为"\001"
			String strReplace = value.toString().replace("\"", "");
			Text txtReplace = new Text();
			txtReplace.set(strReplace);
			value.set(txtReplace.getBytes(), 0, txtReplace.getLength());
			if (newSize == 0) {
				return false;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				return true;
			}
			//System.out.println(strReplace + " , Skipped line of size " + newSize + " at pos "+ (pos - newSize));
		}
		return false;
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable(0);
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

//	public static void main(String[] args) {
//		try {
//			InputStream in = new FileInputStream(new File("/Users/daniel/myFile/CUB/AAAFN1.txt"));
//			EncodeRecordReader m = new EncodeRecordReader(in, 0, 2, 6);
//			m.next(new LongWritable(), new Text("321312$#$4324"));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}

}

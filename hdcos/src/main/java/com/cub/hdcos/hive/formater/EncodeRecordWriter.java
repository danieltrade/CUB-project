package com.cub.hdcos.hive.formater;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EncodeRecordWriter implements RecordWriter {
	
	private RecordWriter writer;
	private BytesWritable bytesWritable;
	//private OutputStream outStream;
	private int finalRowSeparator;
	//org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
//	public EncodeRecordWriter(OutputStream out , int finalRowSeparator) {
//		this.outStream = out;
//		this.finalRowSeparator = finalRowSeparator;
//	}
	
	public EncodeRecordWriter(RecordWriter writer) {
		this.writer = writer;
		bytesWritable = new BytesWritable();
	}
	
	@Override
	public void write(Writable w) throws IOException {
		System.out.println("EncodeRecordWriter write ................ ");
		
		// Get input data
	      byte[] input;
	      int inputLength;
	      if (w instanceof Text) {
	        input = ((Text) w).getBytes();
	        inputLength = ((Text) w).getLength();
	      } else {
	        assert (w instanceof BytesWritable);
	        input = ((BytesWritable) w).getBytes();
	        inputLength = ((BytesWritable) w).getLength();
	      }
	      String text = String.valueOf(input);
	      text += "_encodexxx";
	      byte[] output = text.getBytes();
	      
	      // Encode
	      bytesWritable.set(output, 0, output.length);

	      writer.write(bytesWritable);

	}

	@Override
	public void close(boolean abort) throws IOException {
//		outStream.flush();
//		outStream.close();
		writer.close(abort);
	}
}

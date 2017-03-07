package com.cub.hdcos.hive.formater;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Properties;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
//com.cub.hdcos.hive.formater.EncodeOutputFormat

import com.cub.hdcos.hive.serde.properties.CubInfoTypeaSerDesPropConfig;

public class EncodeOutputFormat<K extends WritableComparable, V extends Writable>
		extends TextOutputFormat<K, V> implements HiveOutputFormat<K, V> {

	@Override
	public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
			Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
			Progressable progress) throws IOException {

		/*
		int rowSeparator = 0;
		String rowSeparatorString = tableProperties.getProperty(serdeConstants.LINE_DELIM, "\n");
		try {
			rowSeparator = Byte.parseByte(rowSeparatorString);
		} catch (NumberFormatException e) {
			rowSeparator = rowSeparatorString.charAt(0);
		}
		final int finalRowSeparator = rowSeparator;
		FileSystem fs = finalOutPath.getFileSystem(jc);
		final OutputStream outStream = Utilities.createCompressedStream(jc, fs.create(finalOutPath, progress),isCompressed);
		final Properties tblprop = tableProperties;
		*/
		FileSystem fs = finalOutPath.getFileSystem(jc);
//		int rowSeparator = 0;
//		String rowSeparatorString = tableProperties.getProperty(serdeConstants.LINE_DELIM, "\n");
//		try {
//			rowSeparator = Byte.parseByte(rowSeparatorString);
//		} catch (NumberFormatException e) {
//			rowSeparator = rowSeparatorString.charAt(0);
//		}
//		final int finalRowSeparator = rowSeparator;
		
		final FSDataOutputStream outStream = fs.create(finalOutPath);
		final Properties tblprop = tableProperties;
		return new RecordWriter() {
			
			@Override
			public void write(Writable r) throws IOException {
				String rowSeparator = tblprop.getProperty(CubInfoTypeaSerDesPropConfig.CUB_DELMITER);
				String encoding = tblprop.getProperty(CubInfoTypeaSerDesPropConfig.CUB_FILE_ENCODING);
				System.out.println("@@@@@ EncodeOutputFormat encoding : " + encoding);
				System.out.println("@@@@@ EncodeOutputFormat finalRowSeparator : " + rowSeparator);
				BufferedWriter out = new BufferedWriter(new OutputStreamWriter(outStream , encoding));
				if (r instanceof Text) {
					Text tr = (Text) r;
					System.out.println("### EncodeOutputFormat write Text : " + tr);
//					outStream.write(tr.getBytes(), 0, tr.getLength());
//					outStream.write(finalRowSeparator);
					out.write(tr.toString());
//					out.write(rowSeparator);
				} else {
					// DynamicSerDe always writes out BytesWritable
					BytesWritable bw = (BytesWritable) r;
					System.out.println("### EncodeOutputFormat write BytesWritable : " + new String(bw.get()));
//					outStream.write(bw.get(), 0, bw.getSize());
//					outStream.write(finalRowSeparator);
					out.write(new String(bw.get()));
//					out.write(rowSeparator);
				}
				out.write("\n");
				out.flush();
			}

			@Override
			public void close(boolean abort) throws IOException {
				outStream.close();
			}
		};
	}
}

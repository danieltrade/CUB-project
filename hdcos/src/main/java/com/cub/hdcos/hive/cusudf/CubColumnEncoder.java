package com.cub.hdcos.hive.cusudf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import com.cub.encrypt.service.CubEncryptUtil;
//com.cub.hdcos.hive.cusudf.CubColumnEncoder
public class CubColumnEncoder extends GenericUDTF {

	private static final Integer OUT_COLS = 2;
	// the output columns size
	private transient Object forwardColObj[] = new Object[OUT_COLS];
//	LazySimpleSerDe
	private transient ObjectInspector encodeType;
	private transient ObjectInspector inputData;

	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		if (argOIs.length != 2) {
			throw new UDFArgumentLengthException("ExplodeMap takes only two argument");
		}
		if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException("ExplodeMap takes string as a parameter");
		}
		if (argOIs[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException("ExplodeMap takes string as a parameter");
		}
		encodeType = argOIs[0];
		inputData = argOIs[1];
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("colVal");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("colEncodeVal");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	public void process(Object[] objects) throws HiveException {
		String encodeTypeStr = ((StringObjectInspector) encodeType).getPrimitiveJavaObject(objects[0]);
		
		// need OI to convert data type to get java type
		String input = ((StringObjectInspector) inputData).getPrimitiveJavaObject(objects[1]);
		forwardColObj[0] = input;
		String encodeValue = "";
		try {
			encodeValue = CubEncryptUtil.getInstance(encodeTypeStr).encodeValue(input);
		} catch (Exception e) {
			e.printStackTrace();
		}
		forwardColObj[1] = encodeValue;
		// output a row with two column
		forward(forwardColObj);
	}

	public void close() throws HiveException {
	}

}

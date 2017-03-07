package com.cub.encrypt.service;

import java.util.HashMap;
import java.util.Map;

import com.cub.encrypt.config.SensitivityCode;
import com.cub.encrypt.entity.EncodeBean;
import com.cub.encrypt.util.StringUtil;

public class CubEncryptUtil {
	
	private static final String SENSITIVITY_ENCRYPT = "sensititvityEncrypt";
	
	private static final String MASK_ENCRYPT = "maskEncrypt";
	
	private static Map<String , EncodeBean> encodeTypeMap; 
	
	private static void initEncodeType() {
		encodeTypeMap = new HashMap<String , EncodeBean>();
		//敏感性個資加密方法
		encodeTypeMap.put(SensitivityCode.A01, new EncodeBean(SensitivityCode.A01 , SENSITIVITY_ENCRYPT , "a01_Encrypt"));
		encodeTypeMap.put(SensitivityCode.A02, new EncodeBean(SensitivityCode.A02 , SENSITIVITY_ENCRYPT , "a02_Encrypt"));
		encodeTypeMap.put(SensitivityCode.A03, new EncodeBean(SensitivityCode.A03 , SENSITIVITY_ENCRYPT , "a03_Encrypt"));
		encodeTypeMap.put(SensitivityCode.A11, new EncodeBean(SensitivityCode.A11 , SENSITIVITY_ENCRYPT , "a11_Encrypt"));
		encodeTypeMap.put(SensitivityCode.A12, new EncodeBean(SensitivityCode.A12 , SENSITIVITY_ENCRYPT , "a12_Encrypt"));
		encodeTypeMap.put(SensitivityCode.A13, new EncodeBean(SensitivityCode.A13 , SENSITIVITY_ENCRYPT , "a13_Encrypt"));
		encodeTypeMap.put(SensitivityCode.A14, new EncodeBean(SensitivityCode.A14 , SENSITIVITY_ENCRYPT , "a14_Encrypt"));
		//需遮罩顯現
		encodeTypeMap.put(SensitivityCode.B11, new EncodeBean(SensitivityCode.B11 , MASK_ENCRYPT , "b11_Encrypt"));
		encodeTypeMap.put(SensitivityCode.B21, new EncodeBean(SensitivityCode.B21 , MASK_ENCRYPT , "b21_Encrypt"));
		encodeTypeMap.put(SensitivityCode.B31, new EncodeBean(SensitivityCode.B31 , MASK_ENCRYPT , "b31_Encrypt"));
		encodeTypeMap.put(SensitivityCode.B41, new EncodeBean(SensitivityCode.B41 , MASK_ENCRYPT , "b41_Encrypt"));
		encodeTypeMap.put(SensitivityCode.B42, new EncodeBean(SensitivityCode.B42 , MASK_ENCRYPT , "b42_Encrypt"));
		encodeTypeMap.put(SensitivityCode.B51, new EncodeBean(SensitivityCode.B51 , MASK_ENCRYPT , "b51_Encrypt"));
		
	}
	
	public static CubEncrypt getInstance(String sensitivityCode) {
		initEncodeType();
		EncodeBean encodeBean = encodeTypeMap.get(sensitivityCode);
		if(encodeBean != null) {
			String encryptType = StringUtil.parseNull(encodeBean.getEncodeType());
			String encodeMethod = StringUtil.parseNull(encodeBean.getEncodeMethod());
			
			if(SENSITIVITY_ENCRYPT.equals(encryptType)) {
				return new SensitivityEncrypt(sensitivityCode , encodeMethod);
			} else if(MASK_ENCRYPT.equals(encryptType)) {
				return new MaskEncrypt(sensitivityCode , encodeMethod);
			}
		}
		return new CubEncrypt() {
			public String encodeValue(String text) throws Exception {
				return text;
			}
		};
	}
	
}

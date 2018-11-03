package com.xx.stdb.engine.hbase.util;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author dux(duxionggis@126.com)
 */
public class EHConstants {
	private EHConstants() {
	}

	public static final String DEF_COL_FAMILY = "CF";
	public static final String DEF_COL_CELL = "S";

	public static final byte[] DEF_COL_FAMILY_B = Bytes.toBytes(DEF_COL_FAMILY);
	public static final byte[] DEF_COL_CELL_B = Bytes.toBytes(DEF_COL_CELL);

	public static final String DEF_SUFFIX_FID = "fid";
	public static final String DEF_SUFFIX_STI = "sti";
	public static final String DEF_TAB_SUFFIX_FID = "_fid_v1";

	static final String DEF_KEY_SCHEMA = "feature.schema";
	static final String DEF_KEY_STI = "feature.sti";
	static final String DEF_KEY_TAB_FID = "collection.fid";
	static final String DEF_KEY_TAB_STI = "collection.sti";

	public static final byte[] DEF_KEY_SCHEMA_B = Bytes.toBytes(DEF_KEY_SCHEMA);
	public static final byte[] DEF_KEY_STI_B = Bytes.toBytes(DEF_KEY_STI);
	public static final byte[] DEF_KEY_TAB_FID_B = Bytes.toBytes(DEF_KEY_TAB_FID);
	public static final byte[] DEF_KEY_TAB_STI_B = Bytes.toBytes(DEF_KEY_TAB_STI);

	public static final boolean KRYO_USED = false; // self bytes is smaller
	public static final boolean FID_BUFFERED = false; // rowkey contains fid

	public static int LIMIT_SCAN_SIZE = 1024; // allow change size

	/**
	 * hbase row key
	 * 
	 * @param code
	 *            String, spatio-temporal code
	 * @param fid
	 *            String, feature id
	 * @return String
	 */
	public static String getRowKey(String code, String fid) {
		return code + ":" + fid; // TODO new alg
	}

	public static String getFid(String rowKey) {
		return rowKey.substring(rowKey.indexOf(":") + 1); // TODO new alg
	}

	public static String getSTCode(String rowKey) {
		return rowKey.substring(0, rowKey.indexOf(":")); // TODO new alg
	}

}

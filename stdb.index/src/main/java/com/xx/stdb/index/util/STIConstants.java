package com.xx.stdb.index.util;

import java.text.SimpleDateFormat;

import com.xx.stdb.index.IndexZOrder;
import com.xx.stdb.index.IndexEnums.IndexType;
import com.xx.stdb.index.IndexHilbert;

/**
 * @author dux(duxionggis@126.com)
 */
public class STIConstants {
	private STIConstants() {
	}

	public static final SimpleDateFormat SDF_HIGH = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	public static final SimpleDateFormat SDF_MID = new SimpleDateFormat("yyyyMMddHHmmss");
	public static final SimpleDateFormat SDF_LOW = new SimpleDateFormat("yyyyMMddHHmm");

	public static final String DATE_HIGH = "19700101080000000";
	public static final String DATE_MID = "19700101080000";
	public static final String DATE_LOW = "197001010800";

	public static final double INTERVAL_HIGH = 1.0e-5;
	public static final double INTERVAL_MID = 2.0e-5;
	public static final double INTERVAL_LOW = 2.0e-3;

	public static final int ZORDER_PREC_HIGH = 12;
	public static final int ZORDER_PREC_MID = 11;
	public static final int ZORDER_PREC_LOW = 5;

	public static final int HILBERT_PREC_HIGH = 30;
	public static final int HILBERT_PREC_MID = 28;
	public static final int HILBERT_PREC_LOW = 11;

	public static String getVersion(IndexType type) {
		if (type == IndexType.ZORDER) {
			return IndexZOrder.getVersion();
		} else {
			return IndexHilbert.getVersion();
		}
	}

	/**
	 * spatio-temporal code
	 * 
	 * @param token
	 *            geohash or hilbert token
	 * @param date
	 *            SimpleDateFormat string
	 * @return String
	 */
	public static String getCode(String token, String date) {
		return token + date; // TODO new alg
	}

}

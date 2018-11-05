package com.xx.stdb.index.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.xx.stdb.index.IndexZOrder;
import com.xx.stdb.index.IndexEnums.IndexType;
import com.xx.stdb.index.IndexHilbert;

/**
 * @author dux(duxionggis@126.com)
 */
public class STIConstants {
	private static Date defDate;

	private STIConstants() {
	}

	public static final SimpleDateFormat SDF_MILLIS = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	public static final SimpleDateFormat SDF_SECOND = new SimpleDateFormat("yyyyMMddHHmmss");
	public static final SimpleDateFormat SDF_MINUTE = new SimpleDateFormat("yyyyMMddHHmm");
	public static final SimpleDateFormat SDF_HOUR = new SimpleDateFormat("yyyyMMddHH");
	public static final SimpleDateFormat SDF_DAY = new SimpleDateFormat("yyyyMMdd");
	public static final SimpleDateFormat SDF_MONTH = new SimpleDateFormat("yyyyMM");

	public static final SimpleDateFormat SDF_HIGH = SDF_MILLIS;
	public static final SimpleDateFormat SDF_MID = SDF_SECOND;
	public static final SimpleDateFormat SDF_LOW = SDF_MINUTE;

	public static final double INTERVAL_HIGH = 1.0e-5;
	public static final double INTERVAL_MID = 2.0e-5;
	public static final double INTERVAL_LOW = 2.0e-3;

	public static final int ZORDER_PREC_HIGH = 12;
	public static final int ZORDER_PREC_MID = 11;
	public static final int ZORDER_PREC_LOW = 5;

	public static final int HILBERT_PREC_HIGH = 30;
	public static final int HILBERT_PREC_MID = 28;
	public static final int HILBERT_PREC_LOW = 11;

	static {
		try {
			defDate = SDF_HIGH.parse("19700101080000000");
		} catch (ParseException e) {
			// TODO log
		}
	}

	public static Date defaultDate() {
		return defDate;
	}

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

	public static String getDate(String code, int dateLen) {
		return code.substring(code.length() - dateLen); // TODO new alg
	}

}

package com.xx.stdb.index;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.xx.stdb.index.IndexEnums.IndexLevel;
import com.xx.stdb.index.IndexEnums.IndexType;
import com.xx.stdb.index.util.STIConstants;

/**
 * @author dux(duxionggis@126.com)
 */
public class IndexPrecision {
	private SimpleDateFormat dateFormat;
	private String dateStr;
	private int precision;
	private double interval;

	IndexPrecision(SimpleDateFormat dateFormat, String dateStr, int precision, double interval) {
		this.dateFormat = dateFormat;
		this.dateStr = dateStr;
		this.precision = precision;
		this.interval = interval;
	}

	public SimpleDateFormat getDateFormat() {
		return dateFormat;
	}

	public String getDateStr() {
		return dateStr;
	}

	public String getDateStr(Date date) {
		return date == null ? this.dateStr : this.dateFormat.format(date);
	}

	public int getPrecision() {
		return precision;
	}

	public double getInterval() {
		return interval;
	}

	/**
	 * create spatio-temporal index code using some properties by IndexPrecision
	 * 
	 * @param type
	 *            IndexType
	 * @param level
	 *            IndexLevel
	 * @return IndexPrecision
	 */
	public static IndexPrecision getPrecision(IndexType type, IndexLevel level) {
		if (type == IndexType.ZORDER) {
			if (level == IndexLevel.HIGH) {
				return new IndexPrecision(STIConstants.SDF_HIGH, STIConstants.DATE_HIGH, STIConstants.ZORDER_PREC_HIGH,
						STIConstants.INTERVAL_HIGH);
			} else if (level == IndexLevel.MID) {
				return new IndexPrecision(STIConstants.SDF_MID, STIConstants.DATE_MID, STIConstants.ZORDER_PREC_MID,
						STIConstants.INTERVAL_MID);
			} else {
				return new IndexPrecision(STIConstants.SDF_LOW, STIConstants.DATE_LOW, STIConstants.ZORDER_PREC_LOW,
						STIConstants.INTERVAL_LOW);
			}
		} else {
			if (level == IndexLevel.HIGH) {
				return new IndexPrecision(STIConstants.SDF_HIGH, STIConstants.DATE_HIGH, STIConstants.HILBERT_PREC_HIGH,
						STIConstants.INTERVAL_HIGH);
			} else if (level == IndexLevel.MID) {
				return new IndexPrecision(STIConstants.SDF_MID, STIConstants.DATE_MID, STIConstants.HILBERT_PREC_MID,
						STIConstants.INTERVAL_MID);
			} else {
				return new IndexPrecision(STIConstants.SDF_LOW, STIConstants.DATE_LOW, STIConstants.HILBERT_PREC_LOW,
						STIConstants.INTERVAL_LOW);
			}
		}
	}

}

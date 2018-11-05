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
	private int precision;
	private double interval;

	IndexPrecision(SimpleDateFormat dateFormat, int precision, double interval) {
		this.dateFormat = dateFormat;
		this.precision = precision;
		this.interval = interval;
	}

	public SimpleDateFormat getDateFormat() {
		return dateFormat;
	}

	public String getDateStr(Date date) {
		return date == null ? "" : this.dateFormat.format(date);
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
	static IndexPrecision getPrecision(IndexType type, IndexLevel level) {
		if (type == IndexType.ZORDER) {
			if (level == IndexLevel.HIGH) {
				return new IndexPrecision(STIConstants.SDF_HIGH, STIConstants.ZORDER_PREC_HIGH,
						STIConstants.INTERVAL_HIGH);
			} else if (level == IndexLevel.MID) {
				return new IndexPrecision(STIConstants.SDF_MID, STIConstants.ZORDER_PREC_MID,
						STIConstants.INTERVAL_MID);
			} else {
				return new IndexPrecision(STIConstants.SDF_LOW, STIConstants.ZORDER_PREC_LOW,
						STIConstants.INTERVAL_LOW);
			}
		} else {
			if (level == IndexLevel.HIGH) {
				return new IndexPrecision(STIConstants.SDF_HIGH, STIConstants.HILBERT_PREC_HIGH,
						STIConstants.INTERVAL_HIGH);
			} else if (level == IndexLevel.MID) {
				return new IndexPrecision(STIConstants.SDF_MID, STIConstants.HILBERT_PREC_MID,
						STIConstants.INTERVAL_MID);
			} else {
				return new IndexPrecision(STIConstants.SDF_LOW, STIConstants.HILBERT_PREC_LOW,
						STIConstants.INTERVAL_LOW);
			}
		}
	}

}

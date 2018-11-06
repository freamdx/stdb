package com.xx.stdb.engine.hbase.util;

import com.xx.stdb.index.util.STIConstants;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author dux(duxionggis@126.com)
 */
public class RowRegexUtil {
	private RowRegexUtil() {
	}

	public static String regex(Set<String> codes, Set<String> fids, Date from, Date to, SimpleDateFormat sdf) {
		String code = getRegexCode(codes);
		String fid = getRegexFid(fids);
		String date = getRegexDate(from, to, sdf);
		return EHConstants.getRowKey(STIConstants.getCode(code, date), fid);
	}

	private static String getRegexCode(Set<String> codes) {
		if (codes == null || codes.isEmpty()) {
			return "";
		}

		int len = codes.iterator().next().length();
		if (codes.size() > EHConstants.LIMIT_ROWKEY_SIZE) {
			codes = regexCodes(codes, len - 1);
		}
		int num = len - codes.iterator().next().length();

		StringBuilder sb = new StringBuilder();
		sb.append("(");
		if (num == 0) {
			codes.forEach(v -> sb.append(v).append("|"));
		} else {
			codes.forEach(v -> sb.append(v + "[0-9a-z]{" + num + "}").append("|"));
		}
		sb.delete(sb.length() - 1, sb.length());
		sb.append(")");
		return sb.toString();
	}

	private static String getRegexFid(Set<String> fids) {
		if (fids == null || fids.isEmpty()) {
			return ".+";
		} else {
			StringBuilder sb = new StringBuilder();
			sb.append("(");
			fids.forEach(v -> sb.append(v).append("|"));
			sb.delete(sb.length() - 1, sb.length());
			sb.append(")");
			return sb.toString();
		}
	}

	private static String getRegexDate(Date from, Date to, SimpleDateFormat sdf) {
		int len = sdf.toPattern().length();
		if (from == null || to == null) {
			return "[0-9]{" + len + "}";
		}

		Set<String> dates;
		long tsec = (to.getTime() - from.getTime()) / 1000;
		int tmin = (int) (tsec / 60);
		int thou = tmin / 60;
		int tday = thou / 24;
		Calendar cdr = Calendar.getInstance();
		if (tsec <= 60) {
			dates = regexDate(from, to, sdf, Calendar.SECOND, STIConstants.SDF_SECOND, cdr, 60);
		} else if (tmin <= 60) {
			dates = regexDate(from, to, sdf, Calendar.MINUTE, STIConstants.SDF_MINUTE, cdr, 60);
		} else if (thou <= 24) {
			dates = regexDate(from, to, sdf, Calendar.HOUR_OF_DAY, STIConstants.SDF_HOUR, cdr, 24);
		} else if (tday <= 30) {
			dates = regexDate(from, to, sdf, Calendar.DAY_OF_MONTH, STIConstants.SDF_DAY, cdr, 30);
		} else {
			dates = regexDate(from, to, sdf, Calendar.MONTH, STIConstants.SDF_MONTH, cdr, 12);
		}

		int length = dates.iterator().next().length();
		if (dates.size() > EHConstants.LIMIT_ROWKEY_SIZE) {
			dates = regexCodes(dates, length - 1);
		}
		int num = len - dates.iterator().next().length();

		StringBuilder sb = new StringBuilder();
		sb.append("(");
		if (num == 0) {
			dates.forEach(v -> sb.append(v).append("|"));
		} else {
			dates.forEach(v -> sb.append(v + "[0-9]{" + num + "}").append("|"));
		}
		// sb.append(sdf.format(STIConstants.defaultDate()));
		sb.delete(sb.length() - 1, sb.length());
		sb.append(")");
		return sb.toString();
	}

	private static Set<String> regexDate(Date from, Date to, SimpleDateFormat sdf, int type, SimpleDateFormat sdfType,
			Calendar cdr, int limit) {
		Set<String> dates = new LinkedHashSet<>();
		Date mid;
		boolean typed = sdf.toPattern().length() >= sdfType.toPattern().length();
		for (int i = 0; i <= limit + 1; i++) {
			cdr.setTime(from);
			cdr.add(type, i);
			mid = cdr.getTime();
			if (!to.before(mid)) {
				if (typed) {
					dates.add(sdfType.format(mid));
				} else {
					dates.add(sdf.format(mid));
				}
			} else {
				break;
			}
		}
		return dates;
	}

	private static Set<String> regexCodes(Set<String> codes, int len) {
		Set<String> set = new HashSet<>();
		Iterator<String> it = codes.iterator();
		while (it.hasNext()) {
			set.add(it.next().substring(0, len));
			if (set.size() == EHConstants.LIMIT_ROWKEY_SIZE + 1) {
				break;
			}
		}
		if (set.size() <= EHConstants.LIMIT_ROWKEY_SIZE) {
			return set;
		} else {
			return regexCodes(codes, len - 1);
		}
	}
}

package com.xx.stdb.index.util;

import java.util.List;
import java.util.StringTokenizer;

/**
 * @author dux(duxionggis@126.com)
 */
public class StringUtil {
	private StringUtil() {
	}

	public static final String DEF_SPLIT = ";";

	/**
	 * default split: \t\n\r\f
	 * 
	 * @param input
	 * @return
	 */
	public static String[] split(String input) {
		StringTokenizer st = new StringTokenizer(input);

		String[] r = new String[st.countTokens()];
		int idx = 0;
		while (st.hasMoreTokens()) {
			r[idx] = st.nextToken();
			idx += 1;
		}
		return r;
	}

	public static String[] split(String input, String delim) {
		StringTokenizer st = new StringTokenizer(input, delim);

		String[] r = new String[st.countTokens()];
		int idx = 0;
		while (st.hasMoreTokens()) {
			r[idx] = st.nextToken();
			idx += 1;
		}
		return r;
	}

	public static String combine(List<String> items) {
		StringBuilder sb = new StringBuilder();
		if (items != null && !items.isEmpty()) {
			items.forEach(v -> sb.append(v).append(DEF_SPLIT));
			sb.delete(sb.length() - 1, sb.length());
		}
		return sb.toString();
	}
}

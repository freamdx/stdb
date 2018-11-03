package com.xx.stdb.base.util;

import java.nio.charset.Charset;

/**
 * @author dux(duxionggis@126.com)
 */
public class Constants {
	private Constants() {
	}

	public static final Charset DEF_CHARSET = Charset.forName("UTF-8");
	public static int DEF_BUFFER_SIZE = 4096; // allow change size

}

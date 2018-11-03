package com.xx.stdb.engine;

/**
 * @author dux(duxionggis@126.com)
 */
public class EngineException extends Exception {
	private static final long serialVersionUID = 1L;

	public EngineException(String msg) {
		super(msg);
	}

	public EngineException(String msg, Throwable cause) {
		super(msg, cause);
	}
}

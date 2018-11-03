package com.xx.stdb.base.feature;

import java.util.Iterator;

/**
 * @author dux(duxionggis@126.com)
 */
public abstract class FeatureIterator implements Iterator<Feature> {

	@Override
	public boolean hasNext() {
		throw new UnsupportedOperationException("hasNext");
	}

	@Override
	public Feature next() {
		throw new UnsupportedOperationException("next");
	}

	public void close() {
		throw new UnsupportedOperationException("close");
	}

}

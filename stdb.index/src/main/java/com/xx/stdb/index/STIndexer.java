package com.xx.stdb.index;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.xx.stdb.index.IndexEnums.IndexLevel;
import com.xx.stdb.index.IndexEnums.IndexType;

/**
 * @author dux(duxionggis@126.com)
 */
public class STIndexer {

	private STIndexer() {
	}

	/**
	 * create indexer(IndexZOrder or IndexHilbert)
	 * 
	 * @param type
	 *            IndexType
	 * @param level
	 *            IndexLevel
	 * @return ISTIndex
	 */
	public static ISTIndex createIndexer(IndexType type, IndexLevel level) {
		if (type == IndexType.ZORDER) {
			return new IndexZOrder(level);
		} else {
			return new IndexHilbert(level);
		}
	}

	public static class STIComparator implements Comparator<String> {
		private static List<String> sources;

		static {
			sources = new ArrayList<>();
			sources.add(IndexType.ZORDER.name() + "_" + IndexLevel.LOW.name());
			sources.add(IndexType.HILBERT.name() + "_" + IndexLevel.LOW.name());
			sources.add(IndexType.ZORDER.name() + "_" + IndexLevel.MID.name());
			sources.add(IndexType.HILBERT.name() + "_" + IndexLevel.MID.name());
			sources.add(IndexType.ZORDER.name() + "_" + IndexLevel.HIGH.name());
			sources.add(IndexType.HILBERT.name() + "_" + IndexLevel.HIGH.name());
		}

		@Override
		public int compare(String sti1, String sti2) {
			int idx1 = getIndex(sti1.toUpperCase());
			int idx2 = getIndex(sti2.toUpperCase());
			if (idx1 <= idx2) {
				return 1;
			} else {
				return -1;
			}
		}

		private int getIndex(String s) {
			int idx = -1;
			for (int i = 0; i < sources.size(); i++) {
				if (s.contains(sources.get(i))) {
					idx = i;
					break;
				}
			}
			return idx;
		}
	}

	public static class STIndexComparator implements Comparator<ISTIndex> {
		private static List<String> sources;

		static {
			sources = new ArrayList<>();
			sources.add(IndexType.ZORDER.name() + "_" + IndexLevel.LOW.name());
			sources.add(IndexType.HILBERT.name() + "_" + IndexLevel.LOW.name());
			sources.add(IndexType.ZORDER.name() + "_" + IndexLevel.MID.name());
			sources.add(IndexType.HILBERT.name() + "_" + IndexLevel.MID.name());
			sources.add(IndexType.ZORDER.name() + "_" + IndexLevel.HIGH.name());
			sources.add(IndexType.HILBERT.name() + "_" + IndexLevel.HIGH.name());
		}

		@Override
		public int compare(ISTIndex sti1, ISTIndex sti2) {
			int idx1 = sources.indexOf(sti1.toString());
			int idx2 = sources.indexOf(sti2.toString());
			if (idx1 <= idx2) {
				return 1;
			} else {
				return -1;
			}
		}

	}

}

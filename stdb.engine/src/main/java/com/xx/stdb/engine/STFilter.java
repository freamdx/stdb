package com.xx.stdb.engine;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.locationtech.jts.geom.Geometry;

/**
 * @author dux(duxionggis@126.com)
 */
public class STFilter {
	private Geometry geometry;
	private Date dateFrom;
	private Date dateTo;
	private Set<String> fids;

	public STFilter(Geometry geometry, Date dateFrom, Date dateTo) {
		this.geometry = geometry;
		this.dateFrom = dateFrom;
		this.dateTo = dateTo;
		this.fids = new HashSet<>();
	}

	public Geometry getGeometry() {
		return geometry;
	}

	public void setGeometry(Geometry geometry) {
		this.geometry = geometry;
	}

	public Date getDateFrom() {
		return dateFrom;
	}

	public void setDateFrom(Date dateFrom) {
		this.dateFrom = dateFrom;
	}

	public Date getDateTo() {
		return dateTo;
	}

	public void setDateTo(Date dateTo) {
		this.dateTo = dateTo;
	}

	public Set<String> getFids() {
		return fids;
	}

	public void addFid(String fid) {
		if (fid != null && !fid.isEmpty()) {
			fids.add(fid);
		}
	}

}

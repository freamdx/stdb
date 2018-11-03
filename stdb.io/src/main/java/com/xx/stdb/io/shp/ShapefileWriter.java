package com.xx.stdb.io.shp;

import com.vivid.shapefile.EndianDataOutputStream;
import com.vivid.shapefile.dbf.DbfFieldDef;
import com.vivid.shapefile.dbf.DbfFile;
import com.vivid.shapefile.dbf.DbfFileWriter;
import com.vivid.shapefile.shp.Shapefile;
import com.xx.stdb.base.feature.*;
import com.xx.stdb.io.CompressedFile;
import com.xx.stdb.io.IDataWriter;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.*;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

/**
 * shape file writer
 * 
 * @author dux(duxionggis@126.com)
 */
public class ShapefileWriter implements IDataWriter {

	public void write(FeatureCollection fc, Properties properties) throws Exception {
		this.checkIfGeomsAreMixed(fc);

		String shpfileName = properties.getProperty(CompressedFile.FILE_KEY).toLowerCase();
		String path = shpfileName.substring(0, shpfileName.lastIndexOf(".shp"));
		String dbffileName = path + ".dbf";
		String shxfileName = path + ".shx";

		this.writeDbf(fc, dbffileName);
		GeometryCollection gc = this.makeSHAPEGeometryCollection(fc);

		int shapeType = 2; // x,y
		String st = properties.getProperty(CompressedFile.SHAPE_TYPE_KEY);
		if (st != null) {
			if (st.equalsIgnoreCase("xy")) {
				shapeType = 2;
			} else if (st.equalsIgnoreCase("xym")) {
				shapeType = 3;
			} else if (st.equalsIgnoreCase("xymz")) {
				shapeType = 4;
			} else if (st.equalsIgnoreCase("xyzm")) {
				shapeType = 4;
			} else if (st.equalsIgnoreCase("xyz")) {
				shapeType = 4;
			} else {
				throw new Exception("'ShapeType' isnt 'xy','xym',or 'xymz'");
			}
		} else {
			if (gc.getNumGeometries() > 0) {
				shapeType = this.guessCoorinateDims(gc.getGeometryN(0));
			}
		}
		Shapefile myshape = new Shapefile();
		myshape.setOutput(new FileOutputStream(shpfileName));
		myshape.write(gc, shapeType);

		BufferedOutputStream in = new BufferedOutputStream(new FileOutputStream(shxfileName));
		EndianDataOutputStream sfile = new EndianDataOutputStream(in);
		myshape.writeIndex(gc, sfile, shapeType);
	}

	/**
	 * Returns: <br>
	 * 2 for 2d (default) <br>
	 * 4 for 3d - one of the oordinates has a non-NaN z value <br>
	 * (3 is for x,y,m but thats not supported yet) <br>
	 * 
	 * @param g
	 *            geometry to test - looks at 1st coordinate
	 **/
	int guessCoorinateDims(Geometry g) {
		Coordinate[] cs = g.getCoordinates();

		for (int t = 0; t < cs.length; t++) {
			if (!(Double.isNaN(cs[t].getZ()))) {
				return 4;
			}
		}

		return 2;
	}

	/**
	 * Write a dbf file with the information from the featureCollection.
	 * 
	 * @param fc
	 *            column data from collection
	 * @param fname
	 *            name of the dbf file to write to
	 */
	void writeDbf(FeatureCollection fc, String fname) throws Exception {
		Schema fs = fc.getSchema();
		DbfFieldDef[] fields = new DbfFieldDef[fs.getAttributeCount()];

		int idx = 0;
		for (int t = 0; t < fs.getAttributeCount(); t++) {
			AttributeType columnType = fs.getAttributeType(t);
			String columnName = fs.getAttributeName(t);

			if (columnType == AttributeType.INTEGER) {
				fields[idx] = new DbfFieldDef(columnName, 'N', 16, 0);
				idx++;
			} else if (columnType == AttributeType.LONG || columnType == AttributeType.FLOAT
					|| columnType == AttributeType.DOUBLE) {
				fields[idx] = new DbfFieldDef(columnName, 'N', 33, 16);
				idx++;
			} else if (columnType == AttributeType.STRING) {
				int maxlength = findMaxStringLength(fc, t);

				if (maxlength > 255) {
					throw new Exception("unsupported strings longer than 255");
				}

				fields[idx] = new DbfFieldDef(columnName, 'C', maxlength, 0);
				idx++;
			} else if (columnType == AttributeType.DATE) {
				fields[idx] = new DbfFieldDef(columnName, 'D', 8, 0);
				idx++;
			} else {
				throw new Exception("unsupported AttributeType");
			}
		}

		// write header
		DbfFileWriter dbf = new DbfFileWriter(fname, CompressedFile.DEFAULT_CHARSET_KEY);
		dbf.writeHeader(fields, fc.size());

		// write rows
		for (Iterator<Feature> it = fc.iterator(); it.hasNext();) {
			Feature f = it.next();
			Vector<Object> DBFrow = new Vector<Object>();
			for (int u = 0; u < fs.getAttributeCount(); u++) {
				AttributeType columnType = fs.getAttributeType(u);
				if (columnType == AttributeType.INTEGER) {
					Object a = f.getAttribute(u);
					if (a == null) {
						DBFrow.add(new Integer(0));
					} else {
						DBFrow.add((Integer) a);
					}
				} else if (columnType == AttributeType.LONG || columnType == AttributeType.FLOAT) {
					String a = f.getString(u);
					if (a == null || a.isEmpty()) {
						DBFrow.add(new Double(0.0));
					} else {
						DBFrow.add(Double.valueOf(a));
					}
				} else if (columnType == AttributeType.DOUBLE) {
					Object a = f.getAttribute(u);
					if (a == null) {
						DBFrow.add(new Double(0.0));
					} else {
						DBFrow.add((Double) a);
					}
				} else if (columnType == AttributeType.STRING) {
					DBFrow.add(f.getString(u));
				} else if (columnType == AttributeType.DATE) {
					Object a = f.getAttribute(u);
					if (a == null) {
						DBFrow.add("");
					} else {
						DBFrow.add(DbfFile.DATE_PARSER.format((Date) a));
					}
				}
			}
			dbf.writeRecord(DBFrow);
		}

		dbf.close();
	}

	/**
	 * look at all the data in the column of the featurecollection, and find the
	 * largest string!
	 * 
	 * @param fc
	 *            features to look at
	 * @param attributeNumber
	 *            which of the column to test.
	 */
	int findMaxStringLength(FeatureCollection fc, int attributeNumber) {
		int maxlen = 0;
		for (Iterator<Feature> it = fc.iterator(); it.hasNext();) {
			Feature f = it.next();
			int l = f.getString(attributeNumber).getBytes().length;

			if (l > maxlen) {
				maxlen = l;
			}
		}

		return Math.max(1, maxlen);
	}

	/**
	 * Find the generic geometry type of the feature collection. Simple method -
	 * find the 1st non null geometry and its type is the generic type. returns
	 * 0 - all empty/invalid <br>
	 * 1 - point <br>
	 * 2 - line <br>
	 * 3 - polygon <br>
	 * 
	 * @param fc
	 *            feature collection containing tet geometries.
	 **/
	int findBestGeometryType(FeatureCollection fc) {
		int type = 0;

		for (Iterator<Feature> it = fc.iterator(); it.hasNext();) {
			Geometry geom = it.next().getGeometry();
			if (geom instanceof Point) {
				type = -1;
			} else if (geom instanceof MultiPoint) {
				return 1;
			} else if (geom instanceof Polygon) {
				return 3;
			} else if (geom instanceof MultiPolygon) {
				return 3;
			} else if (geom instanceof LineString) {
				return 2;
			} else if (geom instanceof MultiLineString) {
				return 2;
			}
		}

		return type;
		// return 0 if all geometries are null
		// return -1 if all geometries are single point
	}

	/**
	 * check first if features are of different geometry type.
	 */
	void checkIfGeomsAreMixed(FeatureCollection fc) throws Exception {
		int idx = 0;
		Class<? extends Geometry> firstClass = null;
		Geometry firstGeom = null;
		for (Iterator<Feature> it = fc.iterator(); it.hasNext();) {
			Feature f = it.next();
			if (idx == 0) {
				firstClass = f.getGeometry().getClass();
				firstGeom = f.getGeometry();
			} else {
				if (firstClass != f.getGeometry().getClass()) {
					if ((firstGeom instanceof Polygon) && (f.getGeometry() instanceof MultiPolygon)) {
						// everything is ok
					} else if ((firstGeom instanceof MultiPolygon) && (f.getGeometry() instanceof Polygon)) {
						// everything is ok
					} else if ((firstGeom instanceof Point) && (f.getGeometry() instanceof MultiPoint)) {
						// everything is ok
					} else if ((firstGeom instanceof MultiPoint) && (f.getGeometry() instanceof Point)) {
						// everything is ok
					} else if ((firstGeom instanceof LineString) && (f.getGeometry() instanceof MultiLineString)) {
						// everything is ok
					} else if ((firstGeom instanceof MultiLineString) && (f.getGeometry() instanceof LineString)) {
						// everything is ok
					} else {
						throw new Exception(
								"mixed geometry types found, please separate Polygons from Lines and Points when saving to *.shp");
					}
				}
			}
			idx++;
		}
	}

	/**
	 * make sure outer ring is CCW and holes are CW
	 * 
	 * @param p
	 *            polygon to check
	 */
	Polygon makeGoodSHAPEPolygon(Polygon p) {
		LinearRing outer;
		LinearRing[] holes = new LinearRing[p.getNumInteriorRing()];
		Coordinate[] coords = p.getExteriorRing().getCoordinates();

		if (Orientation.isCCW(coords)) {
			outer = (LinearRing) p.getExteriorRing().reverse();
		} else {
			outer = (LinearRing) p.getExteriorRing();
		}

		for (int t = 0; t < p.getNumInteriorRing(); t++) {
			coords = p.getInteriorRingN(t).getCoordinates();
			if (!(Orientation.isCCW(coords))) {
				holes[t] = (LinearRing) p.getInteriorRingN(t).reverse();
			} else {
				holes[t] = (LinearRing) p.getInteriorRingN(t);
			}
		}

		return p.getFactory().createPolygon(outer, holes);
	}

	/**
	 * make sure outer ring is CCW and holes are CW for all the polygons in the
	 * Geometry
	 * 
	 * @param mp
	 *            set of polygons to check
	 */
	MultiPolygon makeGoodSHAPEMultiPolygon(MultiPolygon mp) {
		Polygon[] ps = new Polygon[mp.getNumGeometries()];

		// check each sub-polygon
		for (int t = 0; t < mp.getNumGeometries(); t++) {
			ps[t] = makeGoodSHAPEPolygon((Polygon) mp.getGeometryN(t));
		}

		return mp.getFactory().createMultiPolygon(ps);
	}

	/**
	 * return a single geometry collection <Br>
	 * result.GeometryN(i) = the i-th feature in the FeatureCollection<br>
	 * All the geometry types will be the same type (ie. all polygons) - or they
	 * will be set to<br>
	 * NULL geometries<br>
	 * <br>
	 * GeometryN(i) = {Multipoint,Multilinestring, or Multipolygon)<br>
	 * 
	 * @param fc
	 *            feature collection to make homogeneous
	 */
	GeometryCollection makeSHAPEGeometryCollection(FeatureCollection fc) throws Exception {
		int geomtype = this.findBestGeometryType(fc);
		if (geomtype == 0) {
			throw new Exception("Could not determine shapefile type");
		}

		Geometry[] allGeoms = new Geometry[fc.size()];
		int idx = -1;
		for (Iterator<Feature> it = fc.iterator(); it.hasNext();) {
			idx++;
			Geometry geom = it.next().getGeometry();
			switch (geomtype) {
			case -1: // single point
				if ((geom instanceof Point)) {
					allGeoms[idx] = (Point) geom;
				} else {
					allGeoms[idx] = geom.getFactory().createPoint(new Coordinate());
				}
				break;
			case 1: // point
				if ((geom instanceof Point)) {
					Point[] p = new Point[1];
					p[0] = (Point) geom;
					allGeoms[idx] = geom.getFactory().createMultiPoint(p);
				} else if (geom instanceof MultiPoint) {
					allGeoms[idx] = geom;
				} else {
					allGeoms[idx] = geom.getFactory().createMultiPoint(new Point[0]);
				}
				break;
			case 2: // line
				if ((geom instanceof LineString)) {
					LineString[] l = new LineString[1];
					l[0] = (LineString) geom;
					allGeoms[idx] = geom.getFactory().createMultiLineString(l);
				} else if (geom instanceof MultiLineString) {
					allGeoms[idx] = geom;
				} else {
					allGeoms[idx] = geom.getFactory().createMultiLineString(new LineString[0]);
				}
				break;
			case 3: // polygon
				if (geom instanceof Polygon) {
					Polygon[] p = new Polygon[1];
					p[0] = (Polygon) geom;
					allGeoms[idx] = makeGoodSHAPEMultiPolygon(geom.getFactory().createMultiPolygon(p));
				} else if (geom instanceof MultiPolygon) {
					allGeoms[idx] = makeGoodSHAPEMultiPolygon((MultiPolygon) geom);
				} else {
					allGeoms[idx] = geom.getFactory().createMultiPolygon(new Polygon[0]);
				}
				break;
			}
		}

		return allGeoms[0].getFactory().createGeometryCollection(allGeoms);
	}

}

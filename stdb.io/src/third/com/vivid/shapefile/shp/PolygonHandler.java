package com.vivid.shapefile.shp;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import com.vivid.shapefile.EndianDataInputStream;
import com.vivid.shapefile.EndianDataOutputStream;
import org.locationtech.jts.algorithm.CGAlgorithms;
import org.locationtech.jts.geom.*;

public class PolygonHandler implements ShapeHandler {
	GeometryFactory geometryFactory;
	ShapeType myShapeType;

	public PolygonHandler(ShapeType type, GeometryFactory gf)
			throws ShapefileException {
		if ((type != ShapeType.POLYGON) && (type != ShapeType.POLYGONM)
				&& (type != ShapeType.POLYGONZ)) {
			throw new ShapefileException(
					"PolygonHandler constructor - expected type to be 5, 15, or 25.");
		}

		this.myShapeType = type;
		this.geometryFactory = gf;
	}

	// returns true if testPoint is a point in the pointList list.
	/*
	 * boolean pointInList(Coordinate testPoint, Coordinate[] pointList) { int
	 * t, numpoints; Coordinate p;
	 * 
	 * numpoints = Array.getLength( pointList) ; for (t=0;t<numpoints; t++) { p
	 * = pointList[t]; if ( (testPoint.x == p.x) && (testPoint.y == p.y) &&
	 * ((testPoint.z == p.z) || (!(testPoint.z == testPoint.z)) ) //nan test;
	 * x!=x iff x is nan ) { return true; } } return false; }
	 */
	public Geometry read(int contentLength, EndianDataInputStream file)
			throws IOException, ShapefileException {
		int actualReadWords = 0; // actual number of words read (word = 16bits)

		int shapeType = file.readIntLE();
		actualReadWords += 2;

		if (shapeType == 0) {
			return geometryFactory.createMultiPolygon(new Polygon[0]);
		}

		if (shapeType != myShapeType.id) {
			throw new ShapefileException(
					"PolygonHandler.read() - got shape type " + shapeType
							+ " but was expecting " + myShapeType);
		}

		// bounds
		file.readDoubleLE();
		file.readDoubleLE();
		file.readDoubleLE();
		file.readDoubleLE();
		actualReadWords += 4 * 4;

		int partOffsets[];

		int numParts = file.readIntLE();
		int numPoints = file.readIntLE();
		actualReadWords += 4;

		partOffsets = new int[numParts];

		for (int i = 0; i < numParts; i++) {
			partOffsets[i] = file.readIntLE();
			actualReadWords += 2;
		}

		// LinearRing[] rings = new LinearRing[numParts];
		ArrayList<LinearRing> shells = new ArrayList<LinearRing>();
		ArrayList<LinearRing> holes = new ArrayList<LinearRing>();
		Coordinate[] coords = new Coordinate[numPoints];

		for (int t = 0; t < numPoints; t++) {
			coords[t] = new Coordinate(file.readDoubleLE(), file.readDoubleLE());
			actualReadWords += 8;
		}

		if (myShapeType == ShapeType.POLYGONZ) { // PolygonZ
			file.readDoubleLE(); // zmin
			file.readDoubleLE(); // zmax
			actualReadWords += 8;
			for (int t = 0; t < numPoints; t++) {
				coords[t].z = file.readDoubleLE();
				actualReadWords += 4;
			}
		}

		if (myShapeType == ShapeType.POLYGONZ
				|| myShapeType == ShapeType.POLYGONM) {
			// int fullLength = 22 + (2*numParts) + (8*numPoints) + 8 +
			// (4*numPoints)+ 8 + (4*numPoints);
			int fullLength;

			if (myShapeType == ShapeType.POLYGONZ) { // polyZ (with M)
				fullLength = 22 + (2 * numParts) + (8 * numPoints) + 8
						+ (4 * numPoints) + 8 + (4 * numPoints);
			} else { // polyM (with M)
				fullLength = 22 + (2 * numParts) + (8 * numPoints) + 8
						+ (4 * numPoints);
			}

			if (contentLength >= fullLength) {
				file.readDoubleLE(); // mmin
				file.readDoubleLE(); // mmax
				actualReadWords += 8;
				for (int t = 0; t < numPoints; t++) {
					file.readDoubleLE();
					actualReadWords += 4;
				}
			}
		}

		// verify that we have read everything we need
		while (actualReadWords < contentLength) {
			file.readShortBE();
			actualReadWords += 1;
		}

		int offset = 0;
		int start, finish, length;
		for (int part = 0; part < numParts; part++) {
			start = partOffsets[part];
			if (part == numParts - 1) {
				finish = numPoints;
			} else {
				finish = partOffsets[part + 1];
			}
			length = finish - start;
			Coordinate points[] = new Coordinate[length];
			for (int i = 0; i < length; i++) {
				points[i] = coords[offset];
				offset++;
			}
			// REVISIT: polyons with only 1 or 2 points are not polygons -
			// geometryFactory will bomb so we skip if we find one.
			if (points.length == 0 || points.length > 3) {
				LinearRing ring = geometryFactory.createLinearRing(points);
				if (CGAlgorithms.isCCW(points)) {
					holes.add(ring);
				} else {
					shells.add(ring);
				}
			}
		}

		if ((shells.size() > 1) && (holes.size() == 0)) {
			// some shells may be CW holes - esri tolerates this
			holes = findCWHoles(shells, geometryFactory); // find all rings
			// contained in others
			if (holes.size() > 0) {
				shells.removeAll(holes);
				ArrayList<LinearRing> ccwHoles = new ArrayList<LinearRing>(
						holes.size());
				for (int i = 0; i < holes.size(); i++) {
					// ccwHoles.add(reverseRing(holes.get(i)));
					ccwHoles.add((LinearRing) holes.get(i).reverse());
				}
				holes = ccwHoles;
			}
		}

		// now we have a list of all shells and all holes
		ArrayList<ArrayList<LinearRing>> holesForShells = new ArrayList<ArrayList<LinearRing>>(
				shells.size());
		ArrayList<LinearRing> holesWithoutShells = new ArrayList<LinearRing>();

		for (int i = 0; i < shells.size(); i++) {
			holesForShells.add(new ArrayList<LinearRing>());
		}

		// find holes
		for (int i = 0; i < holes.size(); i++) {
			LinearRing testRing = (LinearRing) holes.get(i);
			LinearRing minShell = null;
			Envelope minEnv = null;
			Envelope testEnv = testRing.getEnvelopeInternal();
			Coordinate testPt = testRing.getCoordinateN(0);
			LinearRing tryRing;
			for (int j = 0; j < shells.size(); j++) {
				tryRing = (LinearRing) shells.get(j);
				Envelope tryEnv = tryRing.getEnvelopeInternal();
				if (minShell != null)
					minEnv = minShell.getEnvelopeInternal();
				boolean isContained = false;
				Coordinate[] coordList = tryRing.getCoordinates();

				if (tryEnv.contains(testEnv)
						&& (CGAlgorithms.isPointInRing(testPt, coordList)))
					// && (cga.isPointInRing(testPt,coordList ) ||
					// (pointInList(testPt, coordList))))
					isContained = true;
				// check if this new containing ring is smaller than the current
				// minimum ring
				if (isContained) {
					if (minShell == null || minEnv.contains(tryEnv)) {
						minShell = tryRing;
					}
				}
			}

			if (minShell == null) {
				holesWithoutShells.add(testRing);
			} else {
				holesForShells.get(shells.indexOf(minShell)).add(testRing);
			}
		}

		Polygon[] polygons = new Polygon[shells.size()
				+ holesWithoutShells.size()];
		for (int i = 0; i < shells.size(); i++) {
			polygons[i] = geometryFactory.createPolygon(shells.get(i),
					holesForShells.get(i).toArray(new LinearRing[0]));
		}

		for (int i = 0; i < holesWithoutShells.size(); i++) {
			polygons[shells.size() + i] = geometryFactory.createPolygon(
					holesWithoutShells.get(i), null);
		}

		if (polygons.length == 1) {
			return polygons[0];
		}

		holesForShells = null;
		holesWithoutShells = null;
		shells = null;
		holes = null;

		// its a multi part
		Geometry result = geometryFactory.createMultiPolygon(polygons);
		return result;
	}

	ArrayList<LinearRing> findCWHoles(ArrayList<LinearRing> shells,
			GeometryFactory geometryFactory) {
		ArrayList<LinearRing> holesCW = new ArrayList<LinearRing>(shells.size());
		LinearRing[] noHole = new LinearRing[0];
		for (int i = 0; i < shells.size(); i++) {
			LinearRing iRing = (LinearRing) shells.get(i);
			Envelope iEnv = iRing.getEnvelopeInternal();
			Coordinate[] coordList = iRing.getCoordinates();
			LinearRing jRing;
			for (int j = 0; j < shells.size(); j++) {
				if (i == j)
					continue;
				jRing = (LinearRing) shells.get(j);
				Envelope jEnv = jRing.getEnvelopeInternal();
				Coordinate jPt = jRing.getCoordinateN(0);
				Coordinate jPt2 = jRing.getCoordinateN(1);
				if (iEnv.contains(jEnv)
						// && (CGAlgorithms.isPointInRing(jPt, coordList) ||
						// pointInList(jPt, coordList))
						// && (CGAlgorithms.isPointInRing(jPt2, coordList) ||
						// pointInList(jPt2, coordList))) {
						&& (CGAlgorithms.isPointInRing(jPt, coordList))
						&& (CGAlgorithms.isPointInRing(jPt2, coordList))) {
					if (!holesCW.contains(jRing)) {
						Polygon iPoly = geometryFactory.createPolygon(iRing,
								noHole);
						Polygon jPoly = geometryFactory.createPolygon(jRing,
								noHole);
						if (iPoly.contains(jPoly))
							holesCW.add(jRing);
					}
				}
			}
		}
		return holesCW;
	}

	// /**
	// * reverses the order of points in lr (is CW -> CCW or CCW->CW)
	// */
	// LinearRing reverseRing(LinearRing lr) {
	// int numPoints = lr.getNumPoints();
	// Coordinate[] newCoords = new Coordinate[numPoints];
	// for (int t = 0; t < numPoints; t++) {
	// newCoords[t] = lr.getCoordinateN(numPoints - t - 1);
	// }
	// return new LinearRing(newCoords, new PrecisionModel(), 0);
	// }

	public void write(Geometry geometry, EndianDataOutputStream file)
			throws IOException {
		MultiPolygon multi;
		if (geometry instanceof MultiPolygon) {
			multi = (MultiPolygon) geometry;
		} else {
			multi = geometryFactory
					.createMultiPolygon(new Polygon[] { (Polygon) geometry });
		}
		// file.setLittleEndianMode(true);
		file.writeIntLE(getShapeType().id);

		Envelope box = multi.getEnvelopeInternal();
		file.writeDoubleLE(box.getMinX());
		file.writeDoubleLE(box.getMinY());
		file.writeDoubleLE(box.getMaxX());
		file.writeDoubleLE(box.getMaxY());

		// need to find the total number of rings and points
		int nrings = 0;
		for (int t = 0; t < multi.getNumGeometries(); t++) {
			Polygon p;
			p = (Polygon) multi.getGeometryN(t);
			nrings = nrings + 1 + p.getNumInteriorRing();
		}

		int u = 0;
		int[] pointsPerRing = new int[nrings];
		for (int t = 0; t < multi.getNumGeometries(); t++) {
			Polygon p;
			p = (Polygon) multi.getGeometryN(t);
			pointsPerRing[u] = p.getExteriorRing().getNumPoints();
			u++;
			for (int v = 0; v < p.getNumInteriorRing(); v++) {
				pointsPerRing[u] = p.getInteriorRingN(v).getNumPoints();
				u++;
			}
		}

		int npoints = multi.getNumPoints();

		file.writeIntLE(nrings);
		file.writeIntLE(npoints);

		int count = 0;
		for (int t = 0; t < nrings; t++) {
			file.writeIntLE(count);
			count = count + pointsPerRing[t];
		}

		// write out points here!
		Coordinate[] coords = multi.getCoordinates();
		int num;
		num = Array.getLength(coords);
		for (int t = 0; t < num; t++) {
			file.writeDoubleLE(coords[t].x);
			file.writeDoubleLE(coords[t].y);
		}

		if (myShapeType == ShapeType.POLYGONZ) { // z
			double[] zExtreame = zMinMax(multi);
			if (Double.isNaN(zExtreame[0])) {
				file.writeDoubleLE(0.0);
				file.writeDoubleLE(0.0);
			} else {
				file.writeDoubleLE(zExtreame[0]);
				file.writeDoubleLE(zExtreame[1]);
			}
			for (int t = 0; t < npoints; t++) {
				double z = coords[t].z;
				if (Double.isNaN(z))
					file.writeDoubleLE(0.0);
				else
					file.writeDoubleLE(z);
			}
		}

		if (myShapeType == ShapeType.POLYGONZ
				|| myShapeType == ShapeType.POLYGONM) { // m
			file.writeDoubleLE(-10E40);
			file.writeDoubleLE(-10E40);
			for (int t = 0; t < npoints; t++) {
				file.writeDoubleLE(-10E40);
			}
		}
	}

	double[] zMinMax(Geometry g) {
		boolean validZFound = false;
		Coordinate[] cs = g.getCoordinates();
		double[] result = new double[2];

		double zmin = Double.NaN;
		double zmax = Double.NaN;
		double z;

		for (int t = 0; t < cs.length; t++) {
			z = cs[t].z;
			if (!(Double.isNaN(z))) {
				if (validZFound) {
					if (z < zmin)
						zmin = z;
					if (z > zmax)
						zmax = z;
				} else {
					validZFound = true;
					zmin = z;
					zmax = z;
				}
			}
		}
		result[0] = (zmin);
		result[1] = (zmax);
		return result;
	}

	public ShapeType getShapeType() {
		return myShapeType;
	}

	public int getLength(Geometry geometry) {
		MultiPolygon multi;
		if (geometry instanceof MultiPolygon) {
			multi = (MultiPolygon) geometry;
		} else {
			multi = geometryFactory
					.createMultiPolygon(new Polygon[] { (Polygon) geometry });
		}
		int nrings = 0;
		for (int t = 0; t < multi.getNumGeometries(); t++) {
			Polygon p;
			p = (Polygon) multi.getGeometryN(t);
			nrings = nrings + 1 + p.getNumInteriorRing();
		}

		int npoints = multi.getNumPoints();
		if (myShapeType == ShapeType.POLYGONZ) {
			return 22 + (2 * nrings) + 8 * npoints + 4 * npoints + 8 + 4
					* npoints + 8;
		} else if (myShapeType == ShapeType.POLYGONM) {
			return 22 + (2 * nrings) + 8 * npoints + 4 * npoints + 8;
		}

		return 22 + (2 * nrings) + 8 * npoints;
	}

}

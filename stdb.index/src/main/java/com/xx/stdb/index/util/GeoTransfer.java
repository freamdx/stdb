package com.xx.stdb.index.util;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import ch.hsr.geohash.GeoHash;

/**
 * @author dux(duxionggis@126.com)
 */
public class GeoTransfer {
    private static final GeometryFactory GF = new GeometryFactory();
    private static final int PREC = 12;

    public enum GeoType {
        OP, MP, OL, ML, OR, MR
    }

    private GeoTransfer() {
    }

    /**
     * geohash string transfer to geometry
     *
     * @param hash geohash string
     * @return Geometry
     */
    public static Geometry fromGeoHash(String hash) {
        if (hash == null || hash.isEmpty())
            return null;

        GeoHash gh;
        double lon;
        double lat;
        String gtype = hash.substring(0, 2);
        hash = hash.substring(2);
        if (gtype.equals(GeoType.OP.name())) {
            gh = GeoHash.fromGeohashString(hash);
            lon = format(gh.getPoint().getLongitude());
            lat = format(gh.getPoint().getLatitude());
            return GF.createPoint(new Coordinate(lon, lat));
        } else if (gtype.equals(GeoType.MP.name())) {
            Coordinate[] coords = new Coordinate[hash.length() / PREC];
            for (int i = 0; i < coords.length; i++) {
                gh = GeoHash.fromGeohashString(hash.substring(i * PREC, (i + 1) * PREC));
                lon = format(gh.getPoint().getLongitude());
                lat = format(gh.getPoint().getLatitude());
                coords[i] = new Coordinate(lon, lat);
            }
            return GF.createMultiPointFromCoords(coords);
        } else if (gtype.equals(GeoType.OL.name())) {
            Coordinate[] coords = new Coordinate[hash.length() / PREC];
            for (int i = 0; i < coords.length; i++) {
                gh = GeoHash.fromGeohashString(hash.substring(i * PREC, (i + 1) * PREC));
                lon = format(gh.getPoint().getLongitude());
                lat = format(gh.getPoint().getLatitude());
                coords[i] = new Coordinate(lon, lat);
            }
            return GF.createLineString(coords);
        } else if (gtype.equals(GeoType.ML.name())) {
            String[] hashs = StringUtil.split(hash, ";");
            LineString[] lines = new LineString[hashs.length];
            for (int k = 0; k < hashs.length; k++) {
                Coordinate[] coords = new Coordinate[hashs[k].length() / PREC];
                for (int i = 0; i < coords.length; i++) {
                    gh = GeoHash.fromGeohashString(hashs[k].substring(i * PREC, (i + 1) * PREC));
                    lon = format(gh.getPoint().getLongitude());
                    lat = format(gh.getPoint().getLatitude());
                    coords[i] = new Coordinate(lon, lat);
                }
                lines[k] = GF.createLineString(coords);
            }
            return GF.createMultiLineString(lines);
        } else if (gtype.equals(GeoType.OR.name())) {
            String[] hashs = StringUtil.split(hash, ",");
            LinearRing shell = null;
            LinearRing[] holes = new LinearRing[hashs.length - 1];
            for (int k = 0; k < hashs.length; k++) {
                Coordinate[] coords = new Coordinate[hashs[k].length() / PREC];
                for (int i = 0; i < coords.length; i++) {
                    gh = GeoHash.fromGeohashString(hashs[k].substring(i * PREC, (i + 1) * PREC));
                    lon = format(gh.getPoint().getLongitude());
                    lat = format(gh.getPoint().getLatitude());
                    coords[i] = new Coordinate(lon, lat);
                }
                coords[coords.length - 1] = coords[0];
                if (k == 0) {
                    shell = GF.createLinearRing(coords);
                } else {
                    holes[k - 1] = GF.createLinearRing(coords);
                }
            }
            return GF.createPolygon(shell, holes);
        } else if (gtype.equals(GeoType.MR.name())) {
            String[] phashs = StringUtil.split(hash, ";");
            Polygon[] polys = new Polygon[phashs.length];
            for (int m = 0; m < phashs.length; m++) {
                String[] hashs = StringUtil.split(phashs[m], ",");
                LinearRing shell = null;
                LinearRing[] holes = new LinearRing[hashs.length - 1];
                for (int k = 0; k < hashs.length; k++) {
                    Coordinate[] coords = new Coordinate[hashs[k].length() / PREC];
                    for (int i = 0; i < coords.length; i++) {
                        gh = GeoHash.fromGeohashString(hashs[k].substring(i * PREC, (i + 1) * PREC));
                        lon = format(gh.getPoint().getLongitude());
                        lat = format(gh.getPoint().getLatitude());
                        coords[i] = new Coordinate(lon, lat);
                    }
                    coords[coords.length - 1] = coords[0];
                    if (k == 0) {
                        shell = GF.createLinearRing(coords);
                    } else {
                        holes[k - 1] = GF.createLinearRing(coords);
                    }
                }
                polys[m] = GF.createPolygon(shell, holes);
            }
            return GF.createMultiPolygon(polys);
        }
        return null;
    }

    /**
     * geometry transfer to geohash string
     *
     * @param geo Geometry
     * @return String
     */
    public static String toGeoHash(Geometry geo) {
        if (geo == null || geo.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();
        String gType = geo.getGeometryType();
        if (gType.equals("Point")) {
            sb.append(GeoType.OP.name());
            GeoHash gh = GeoHash.withCharacterPrecision(geo.getCoordinate().y, geo.getCoordinate().x, PREC);
            sb.append(gh.toBase32());
        } else if (gType.equals("MultiPoint")) {
            sb.append(GeoType.MP.name());
            Coordinate[] coords = geo.getCoordinates();
            for (Coordinate coord : coords) {
                sb.append(GeoHash.withCharacterPrecision(coord.y, coord.x, PREC).toBase32());
            }
        } else if (gType.equals("LineString")) {
            sb.append(GeoType.OL.name());
            Coordinate[] coords = geo.getCoordinates();
            for (Coordinate coord : coords) {
                sb.append(GeoHash.withCharacterPrecision(coord.y, coord.x, PREC).toBase32());
            }
        } else if (gType.equals("MultiLineString")) {
            sb.append(GeoType.ML.name());
            int num = geo.getNumGeometries();
            for (int k = 0; k < num; k++) {
                Coordinate[] coords = geo.getGeometryN(k).getCoordinates();
                for (Coordinate coord : coords) {
                    sb.append(GeoHash.withCharacterPrecision(coord.y, coord.x, PREC).toBase32());
                }
                if (k != num - 1) {
                    sb.append(";");
                }
            }
        } else if (gType.equals("Polygon")) {
            sb.append(GeoType.OR.name());
            Polygon poly = (Polygon) geo;
            // Shell
            Coordinate[] coords = poly.getExteriorRing().getCoordinates();
            for (Coordinate coord : coords) {
                sb.append(GeoHash.withCharacterPrecision(coord.y, coord.x, PREC).toBase32());
            }
            // Holes
            int num = poly.getNumInteriorRing();
            for (int k = 0; k < num; k++) {
                sb.append(",");
                coords = poly.getInteriorRingN(k).getCoordinates();
                for (Coordinate coord : coords) {
                    sb.append(GeoHash.withCharacterPrecision(coord.y, coord.x, PREC).toBase32());
                }
            }
        } else if (gType.equals("MultiPolygon")) {
            sb.append(GeoType.MR.name());
            MultiPolygon mpoly = (MultiPolygon) geo;
            for (int m = 0; m < geo.getNumGeometries(); m++) {
                Polygon poly = (Polygon) mpoly.getGeometryN(m);
                // Shell
                Coordinate[] coords = poly.getExteriorRing().getCoordinates();
                for (Coordinate coord : coords) {
                    sb.append(GeoHash.withCharacterPrecision(coord.y, coord.x, PREC).toBase32());
                }
                // Holes
                int num = poly.getNumInteriorRing();
                for (int k = 0; k < num; k++) {
                    sb.append(",");
                    coords = poly.getInteriorRingN(k).getCoordinates();
                    for (Coordinate coord : coords) {
                        sb.append(GeoHash.withCharacterPrecision(coord.y, coord.x, PREC).toBase32());
                    }
                }
                if (m != geo.getNumGeometries() - 1) {
                    sb.append(";");
                }
            }
        } else if (gType.equals("GeometryCollection")) {
            // TODO
        }
        return sb.toString();
    }

    private static double format(double ll) {
        return Math.round(ll * 1.0e7) / 1.0e7;
    }

}

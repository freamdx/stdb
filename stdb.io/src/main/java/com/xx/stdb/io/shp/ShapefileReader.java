package com.xx.stdb.io.shp;

import com.vivid.shapefile.dbf.DbfFile;
import com.vivid.shapefile.shp.Shapefile;
import com.vivid.shapefile.shp.ShapefileException;
import com.xx.stdb.base.feature.AttributeType;
import com.xx.stdb.base.feature.FeatureCollection;
import com.xx.stdb.io.CompressedFile;
import com.xx.stdb.io.IDataReader;
import com.xx.stdb.base.feature.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * shape file reader
 * 
 * @author dux(duxionggis@126.com)
 */
public class ShapefileReader implements IDataReader {
	private File delete_tmp_dbf = null;

	public FeatureCollection read(Properties properties) throws Exception {
		String shpfileName = properties.getProperty(CompressedFile.FILE_KEY).toLowerCase();
		String dbffileName = shpfileName.substring(0, shpfileName.length() - 4) + ".dbf";

		Shapefile myshape = getShapefile(shpfileName, properties.getProperty(CompressedFile.COMPRESSED_FILE_KEY));
		DbfFile mydbf = getDbfFile(dbffileName, properties.getProperty(CompressedFile.COMPRESSED_FILE_KEY));
		GeometryFactory factory = new GeometryFactory();
		GeometryCollection collection = null;
		try {
			collection = myshape.read(factory);
		} catch (ShapefileException ex) {
		} finally {
			myshape.close();
		}
		Schema fs = new Schema();
		FeatureCollection featureCollection = null;
		if (mydbf == null) {
			featureCollection = new FeatureCollection(fs);
			if (collection != null) {
				int numGeometries = collection.getNumGeometries();
				for (int x = 0; x < numGeometries; x++) {
					Feature feature = new Feature(fs);
					feature.setGeometry(collection.getGeometryN(x));
					featureCollection.add(feature);
				}
			}
		} else {
			int numfields = mydbf.getNumFields();
			for (int j = 0; j < numfields; j++) {
				fs.addAttribute(mydbf.getFieldName(j), AttributeType.valueOf(mydbf.getFieldType(j)));
			}
			featureCollection = new FeatureCollection(fs);

			for (int x = 0; x < mydbf.getLastRec(); x++) {
				Feature feature = new Feature(fs);
				Geometry geo = (collection == null ? null : collection.getGeometryN(x));
				// StringBuilder s = mydbf.GetDbfRec(x);
				byte[] s = mydbf.GetDbfRec(x);

				for (int y = 0; y < numfields; y++) {
					feature.setAttribute(y, mydbf.ParseRecordColumn(s, y));
				}

				feature.setGeometry(geo);
				featureCollection.add(feature);
			}

			mydbf.close();
			deleteTmpDbf();
		}

		return featureCollection;
	}

	Shapefile getShapefile(String shpfileName, String compressedFname) throws Exception {
		InputStream in = CompressedFile.openFile(shpfileName, compressedFname);
		Shapefile myshape = new Shapefile();
		myshape.setInput(in);
		return myshape;
	}

	DbfFile getDbfFile(String dbfFileName, String compressedFname) throws Exception {
		DbfFile mydbf = null;
		if ((compressedFname != null) && (compressedFname.length() > 0)) {
			byte[] b = new byte[16000];
			int len;
			boolean keepGoing = true;

			// copy the file then use that copy
			File file = File.createTempFile("dbf", ".dbf");
			FileOutputStream out = new FileOutputStream(file);

			InputStream in = CompressedFile.openFile(dbfFileName, compressedFname);

			while (keepGoing) {
				len = in.read(b);

				if (len > 0) {
					out.write(b, 0, len);
				}

				keepGoing = (len != -1);
			}

			in.close();
			out.close();

			mydbf = new DbfFile(file.toString(), CompressedFile.DEFAULT_CHARSET_KEY);
			delete_tmp_dbf = file; // to be deleted later on
		} else {
			File dbfFile = new File(dbfFileName);

			if (dbfFile.exists()) {
				mydbf = new DbfFile(dbfFileName, CompressedFile.DEFAULT_CHARSET_KEY);
			}
		}

		return mydbf;
	}

	void deleteTmpDbf() {
		if (delete_tmp_dbf != null) {
			delete_tmp_dbf.delete();
			delete_tmp_dbf = null;
		}
	}
}

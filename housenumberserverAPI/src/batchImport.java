

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.*;

import de.regioosm.housenumberserverAPI.Applicationconfiguration;


/**
 * Upload of a housenumber evaluation result file via a multipart/form-data request for the client.
 * The request can contain one file and additionally fields.
 * <p>
 * The multipart/form-data request will be interpreted with the class net.balusc.http.multipart.MultipartMap.
 * Afterwards, the file will be stored first on server file system and send a response http 200 code.
 * <p>Then the result file content will be imported into housenumber database and made public available via the housenumber server at regio-osm.de/hausnummerauswertung
 * 
 */

/*
 * some local test cases to find out, how fast the import of a housenumber evaluation will work
 * server API, based on node.js: insert 22750 housenumber lines (for Würzburg) 
 * 			needed 13.6 minutes without transaction and with asynchronous parallel insert sql statements
 * server API, based on java servlet: insert 22751 housenumber lines (for Würzburg) 
 * 			needed 11,7 minutes without transaction and with serial insert sql statements
 * server API, based on java servlet, insert 22751 housenumber lines (for Würzburg)
 * 			needed 7,9 minutes  in transaction mode and with serial insert sql statements
 * 
 */
	//	parameters for external class, which interpret the client request:
	//	location: to which temporary directory, a file can be created and stored.
	//	maxFileSize: up to which size, the upload file can be. CAUTION: as of 2015-01-11, the upload file is not compressed
public class batchImport {
		// load content of configuration file, which contains filesystem entries and database connection details
	static Applicationconfiguration configuration = new Applicationconfiguration(".");
	static Connection con_hausnummern;

		// storage of streetnames and their internal DB id. If a street is missing in DB, it will be inserted,
		// before the insert of the housenumbers at the streets will be inserted
	static HashMap<String, Integer> street_idlist = new HashMap<String, Integer>();

    /**
     * normalize housenumber to 4 digits, filling some prefix 0 (zeros), to enable sorting of housenumbers, even with literal suffixes
     * <p>
     * @param hausnummer: 	housenumber, optionally with numeric or literal suffixes
     * @return:	housenumber in normalized form, means with leading 0 (zeros). Housenumber "3a" will be returned to "0003a", "157 1/2a" to "0157 1/2a"
     */
    private static String setHausnummerNormalisiert(String hausnummer) {
		String hausnummersortierbar = "";
    	if(hausnummer != "") {
    		int numstellen = 0;
    		for(int posi=0;posi<hausnummer.length();posi++) {
    			int charwert = hausnummer.charAt(posi);
    			if( (charwert >= '0') && (charwert <= '9'))
    				numstellen++;
    			else
    				break;
    		}
    		for(int anzi=0;anzi<(4-numstellen);anzi++)
    			hausnummersortierbar += "0";
    		hausnummersortierbar += hausnummer;
    	}
		return hausnummersortierbar;
    }

    /**
     * store the evaluation result of a municipality with a country into the housenumber DB for public access
     * 
     * @param content: evaluation result. In first line, there should be a header, starting with # and containing columns with header names
     * @param country: country part of identification of result file, together with municipality parameter
     * @param municipality: municipality part of identification of result file.
     * @return: errors in string format
     */
    private static boolean insertResultIntoDB(String content) {
		java.util.Date insertResultIntoDBouter = new java.util.Date();

		java.util.Date evaluationtime = null;
		java.util.Date osmtime = null;

		if(content.length() == 0) {
//TODO print to error log
			System.out.println("leerer Content, Abbruch");
			return true;
		}

		try {
			int countryid = 0;
			String country = "";
			int municipalityid = 0;
			String municipality = "";
			int jobid = 0;
			String jobname = "";
			String officialkeysId = "";
			Integer adminlevel = 0;
			String serverobjectId = "";
			String polygonAsText = "";
			Integer polygonSrid = 0;

			
			String lines[] = content.split("\n");
			// loop over all result file lines
			for(int lineindex = 0; lineindex < lines.length; lineindex++) {
				String actline = lines[lineindex];
				if(actline == "")
					continue;
					// analyse first line of uploaded file, if it contains the line with all file columns
					// ignore other comment lines
				if(actline.indexOf("#") == 0) {
					if(actline.indexOf("#Para ") == 0) {
						String keyvalue[] = actline.substring(6).split("=");
						String key = "";
						String value = "";
						if(keyvalue.length >= 1)
							key = keyvalue[0];
						if(keyvalue.length >= 2)
							value = keyvalue[1];
						System.out.println("Info: found comment line with parameter [" + key + "] ===" + value + "===");
	
						if(key.equals("OSMTime")) {
							osmtime = new java.util.Date(Long.parseLong(value));
						}
						if(key.equals("EvaluationTime")) {
							evaluationtime = new java.util.Date(Long.parseLong(value));
						}
						if(key.equals("Country")) {
							country = value;
						}
						if(key.equals("Municipality")) {
							municipality = value;
						}
						if(key.equals("Adminlevel")) {
							adminlevel = Integer.parseInt(value);
						}

						if(key.equals("Jobname")) {
							jobname = value;
						}
						if(key.equals("Officialkeysid")) {
							officialkeysId = value;
						}
						if(key.equals("Serverobjectid")) {
							serverobjectId = value;
						}
					}
				}
			}

			if(officialkeysId.equals(""))
				officialkeysId = "%";

			Class.forName("org.postgresql.Driver");
	
			String url_hausnummern = configuration.db_application_url;
			con_hausnummern = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);

			System.out.println("beginn von insertfkt");
			
			String select_sql = "SELECT land.id AS countryid, land,"
				+ " stadt.id AS municipalityid, stadt, officialkeys_id, admin_level,"
				+ " ST_AsText(polygon) AS polygon_astext, ST_SRID(polygon) AS polygon_srid,"
				+ " jobs.id AS jobid, jobname"
				+ " FROM land, stadt, gebiete, jobs WHERE"
				+ " land = ?"
				+ " AND stadt = ?"
				+ " AND jobname = ?"
				+ " AND officialkeys_id like ?";
				if(adminlevel != 0)
					select_sql += " AND admin_level = ?";
				select_sql += " AND gebiete.stadt_id = stadt.id"
				+ " AND jobs.gebiete_id = gebiete.id"
				+ " ORDER BY admin_level;";

			PreparedStatement selectqueryStmt = con_hausnummern.prepareStatement(select_sql);
			selectqueryStmt.setString(1, country);
			selectqueryStmt.setString(2, municipality);
			selectqueryStmt.setString(3, jobname);
			selectqueryStmt.setString(4, officialkeysId);
			if(adminlevel != 0)
				selectqueryStmt.setInt(5, adminlevel);
			System.out.println("Info: get municipality data ...");
			ResultSet existingmunicipalityRS = selectqueryStmt.executeQuery();


					//TODO check and code, what happens with more than one row
			Integer countHits = 0;
			StringBuffer moreThanOneRowContent = new StringBuffer();
			while(existingmunicipalityRS.next()) {
				countHits++;
				countryid = existingmunicipalityRS.getInt("countryid");
				country = existingmunicipalityRS.getString("land");
				municipalityid = existingmunicipalityRS.getInt("municipalityid");
				municipality = existingmunicipalityRS.getString("stadt");
				jobid = existingmunicipalityRS.getInt("jobid");
				jobname = existingmunicipalityRS.getString("jobname");
				polygonAsText = existingmunicipalityRS.getString("polygon_astext");
				polygonSrid = existingmunicipalityRS.getInt("polygon_srid");
				moreThanOneRowContent.append(country + "," + municipality + "," + existingmunicipalityRS.getString("officialkeys_id") 
					+ "," + existingmunicipalityRS.getInt("admin_level") + "," + jobname + "\n");
			}

			if(countHits > 1) {
				System.out.println("Error: more than one related jobs were found, CANCEL import ...");
				System.out.println(moreThanOneRowContent.toString());
				selectqueryStmt.close();
				con_hausnummern.close();
				return false;
			} else if(countHits == 0) {
				selectqueryStmt.close();
				System.out.println("Error: There is no matching municipality, the uploaded result will be suspended ...");
				System.out.println("  country ===" + country + "===, municipality ===" 
						+ municipality + "===, officialkeysId ===" + officialkeysId + "===, jobname ===" + jobname + "===");
				selectqueryStmt.close();
				con_hausnummern.close();
				return false;
			}
			
			System.out.println("related DB job: id # " + jobid + "    jobname ===" + jobname + "===   in  " + municipality + ", " + country);
//TODO check tstamp of last evaluation of actual job: tstamp-date must be older than tstamp date from actual result file. Otherwise ignore actual file

			
				// delete up to now active evaluation for same municipality
				// It will be checked against jobname = municipality name to only delete complete municipality evaluation, 
				// not optionally available subadmin evaluations
			String deletelastevaluationSql = "DELETE FROM auswertung_hausnummern"
				+ " WHERE land_id = ? AND stadt_id = ? AND job_id = ?;";
			PreparedStatement deletelastevaluationStmt = con_hausnummern.prepareStatement(deletelastevaluationSql);
			System.out.println("Info: delete old evaluation in table auswertung_hausnummern ===" + deletelastevaluationSql 
					+ "=== with parameters 1 ===" + countryid + "===, 2 ===" + municipalityid + "===, 3 ===" + jobid + "===...");
			try {
				deletelastevaluationStmt.setInt(1, countryid);
				deletelastevaluationStmt.setInt(2, municipalityid);
				deletelastevaluationStmt.setInt(3, jobid);
				deletelastevaluationStmt.executeUpdate();
			}
			catch( SQLException e) {
				System.out.println("ERROR, when tried to delete rows for old evaluation, but import will continue");
				e.printStackTrace();
			}
			deletelastevaluationStmt.close();

				// delete all job entries in exporthnr2shape
			String deleteLastEvaluationMapResultsql = "DELETE FROM exporthnr2shape";
			deleteLastEvaluationMapResultsql  += " WHERE";
			deleteLastEvaluationMapResultsql  += " job_id = ?;";
			PreparedStatement deleteLastEvaluationMapResultStmt = con_hausnummern.prepareStatement(deleteLastEvaluationMapResultsql);
			System.out.println("Info: delete old evaluation in table exporthnr2shape ...");
			try {
				deleteLastEvaluationMapResultStmt.setInt(1, jobid);
				deleteLastEvaluationMapResultStmt.executeUpdate();
			}
			catch( SQLException e) {
				System.out.println("ERROR, when tried to delete rows for old evaluation in exporthnr2shape table, but import will continue");
				e.printStackTrace();
			}
			deleteLastEvaluationMapResultStmt.close();

			String selectallofficialstreetsSql = "SELECT DISTINCT ON (strasse) strasse, strasse.id AS id"
				+ " FROM stadt_hausnummern, strasse, stadt, land"
				+ " WHERE stadt_hausnummern.strasse_id = strasse.id"
				+ " AND stadt_hausnummern.stadt_id = stadt.id"
				+ " AND stadt_hausnummern.land_id = land.id"
				+ " AND stadt = ? AND land = ?;";
			PreparedStatement selectallofficialstreetsStmt = con_hausnummern.prepareStatement(selectallofficialstreetsSql);
			
			int numberStreetsFromOfficialList = 0;
			int numberStreetsLoadedDynamically = 0;
			int numberStreetInsertedIntoDB = 0;
			System.out.println("Info: get all official streets with municipality ...");
			try {
				selectallofficialstreetsStmt.setString(1, municipality);
				selectallofficialstreetsStmt.setString(2, country);
				ResultSet selectallofficialstreetsRS = selectallofficialstreetsStmt.executeQuery();
				while(selectallofficialstreetsRS.next()) {
					if(! street_idlist.containsKey(selectallofficialstreetsRS.getString("strasse"))) {
						street_idlist.put(selectallofficialstreetsRS.getString("strasse"), selectallofficialstreetsRS.getInt("id"));
						numberStreetsFromOfficialList++;
					}
				}
			}
			catch( SQLException e) {
				System.out.println("ERROR, when tried to get all official street names from municipality ===" + municipality + "=== in country ===" + country + "===");
				e.printStackTrace();
			}
			selectallofficialstreetsStmt.close();
			

			String selectStreetidSql = "SELECT id FROM strasse where strasse = ?;";
			PreparedStatement selectStreetidStmt = con_hausnummern.prepareStatement(selectStreetidSql);

			String insertstreetsql = "INSERT INTO strasse (strasse) VALUES (?) returning id;";
			PreparedStatement insertstreetstmt = con_hausnummern.prepareStatement(insertstreetsql);

			String selectOSMStreetGeometrySql = "SELECT strasse_id AS id, strasse, jobs_strassen.linestring AS linestring900913,"
				+ " osm_ids, ST_IsClosed(jobs_strassen.linestring) AS linestring_isclosed"
				+ " FROM jobs_strassen JOIN strasse"
				+ "   ON jobs_strassen.strasse_id = strasse.id"
				+ " WHERE job_id = ? AND"
				+ " strasse_id = ?"
				+ " ORDER BY correctorder(strasse);";
			PreparedStatement selectOSMStreetGeometryStmt = con_hausnummern.prepareStatement(selectOSMStreetGeometrySql);

			String insertStreetResultSql = "INSERT INTO exporthnr2shape"
				+ " (land_id, stadt_id, job_id, strasse, hnr_soll, hnr_osm,"
				+ " hnr_fhlosm, hnr_nurosm, hnr_abdeck, hnr_liste, timestamp, geom)"
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ST_Transform(?::geometry, 4326)"
				+ ");";
			PreparedStatement insertStreetResultStmt = con_hausnummern.prepareStatement(insertStreetResultSql);

			String inserthousenumberWithGeometrySql = "INSERT INTO auswertung_hausnummern"
				+ " (land_id, stadt_id, job_id, copyland, copystadt, copyjobname, copystrasse,"
				+ " strasse_id, postcode, hausnummer, hausnummer_sortierbar, treffertyp,"
				+ " osm_objektart, osm_id, objektart, point"
				+ ")"
				+ " VALUES(?, ?, ?, ?, ?, ?, ?,"
				+ " ?, ?, ?, ?, ?,"
				+ " ?, ?, ?, ST_Setsrid(ST_Makepoint(?, ?), 4326));";
			PreparedStatement inserthousenumberWithGeometryStmt = con_hausnummern.prepareStatement(inserthousenumberWithGeometrySql);

			String inserthousenumberWithoutGeometrySql = "INSERT INTO auswertung_hausnummern"
				+ " (land_id, stadt_id, job_id, copyland, copystadt, copyjobname, copystrasse,"
				+ " strasse_id, postcode, hausnummer, hausnummer_sortierbar, treffertyp,"
				+ " osm_objektart, osm_id, objektart"
				+ ")"
				+ " VALUES(?, ?, ?, ?, ?, ?, ?,"
				+ " ?, ?, ?, ?, ?,"
				+ " ?, ?, ?);";
			PreparedStatement inserthousenumberWithoutGeometryStmt = con_hausnummern.prepareStatement(inserthousenumberWithoutGeometrySql);

			
				// change to transaction mode
			con_hausnummern.setAutoCommit(false);

				// store column names, below found in file header line
			HashMap<Integer, String> headerfield = new HashMap<Integer, String>();

			Integer street_hittype_l = 0;
			Integer street_hittype_i = 0;
			Integer street_hittype_o = 0;
			String actStreet = "";
			String previousStreet = "";
			StringBuffer actStreetMissingHousenumbers = new StringBuffer();

				// loop over all result file lines
			int countHousenumbersIdentical = 0;
			int countHousenumbersListonly = 0;
			int countHousenumbersOsmonly = 0;
			java.util.Date loopstart = new java.util.Date();
			for(int lineindex = 0; lineindex < lines.length; lineindex++) {
				previousStreet = actStreet;

				if((lineindex % 1000) == 0)
					System.out.println("read result file line no " + lineindex);

				String actline = lines[lineindex];
				if(actline == "")
					continue;
					// analyse first line of uploaded file, if it contains the line with all file columns
				if((lineindex == 0) && (actline.toLowerCase().indexOf("#strasse") == 0)) {
					actline = actline.substring(1);
					String columns[] = actline.toLowerCase().split("\t");
					for(int colindex = 0; colindex < columns.length; colindex++) {
						String actcolumn = columns[colindex];
						if(actcolumn == "")
							continue;
						headerfield.put(colindex, actcolumn);
						//System.out.println("stored headerfield[" + colindex + "] ===" + actcolumn + "===");
					}
					continue;
				}
					// ignore other comment lines
				if(actline.indexOf("#") == 0) {
					if(actline.indexOf("#Para ") == 0) {
						String keyvalue[] = actline.substring(6).split("=");
						String key = "";
						String value = "";
						if(keyvalue.length >= 1)
							key = keyvalue[0];
						if(keyvalue.length >= 2)
							value = keyvalue[1];
						if(key.equals("OSMTime")) {
							osmtime = new java.util.Date(Long.parseLong(value));
						}
						if(key.equals("EvaluationTime")) {
							evaluationtime = new java.util.Date(Long.parseLong(value));
						}
						if(key.equals("Country")) {
							country = value;
						}
						if(key.equals("Municipality")) {
							municipality = value;
						}
						if(key.equals("Jobname")) {
							jobname = value;
						}
					}
					continue;
				}
				String columns[] = actline.split("\t");
				if(columns.length < 6) {
					System.out.println("Error: file line contains to less columns, will be ignored. line no.: " + lineindex + "  line content ===" + actline + "===");
					continue;
				}
					// associate columns of actual file line with header column names to easy take the columns
				HashMap<String, String> field = new HashMap<String, String>();
				for(int colindex = 0; colindex < columns.length; colindex++) {
					String actcolumn = columns[colindex];
					if(actcolumn == "")
						continue;
					field.put(headerfield.get(colindex), actcolumn);
					//System.out.println("stored field[" + headerfield.get(colindex) + "] ===" + actcolumn + "===");
				}

				String actPostcode = "";
				if(field.get("postcode") != null)
					actPostcode = field.get("postcode");

				actStreet = "";
				if(field.get("strasse") != null)
					actStreet = field.get("strasse");
				//System.out.println("actStreet ===" + actStreet + "===");

				String acthousenumber = "";
				if(field.get("hausnummer") != null)
					acthousenumber = field.get("hausnummer");
				//System.out.println("acthousenumber ===" + acthousenumber + "===");
				String acthousenumbersorted = setHausnummerNormalisiert(acthousenumber);
				//System.out.println("acthousenumbersorted ===" + acthousenumbersorted + "===");

				String acthittype = "";
				if(field.get("treffertyp") != null)
					acthittype = field.get("treffertyp");
				//System.out.println("acthittype ===" + acthittype + "===");

				String actosmtag = "";	// OSM "key"=>"value" of the osm object, which holds the housenumber
				if(field.get("osmtag") != null)
					actosmtag = field.get("osmtag");
				//System.out.println("actosmtag ===" + actosmtag + "===");

				String actosmtype = "";	// will be "node", "way" or "relation"
				if(field.get("osmtyp") != null)
					actosmtype = field.get("osmtyp");
				//System.out.println("actosmtype ===" + actosmtype + "===");

				Long actosmid = -1L;	// OSM id of the osm objects, which holds the housenumber. It must be used in conjunction with actosmtype
				if(field.get("osmid") != null)
					actosmid = Long.parseLong(field.get("osmid"));
				//System.out.println("actosmid ===" + actosmid + "===");

				double actlon = 0.0D;
				double actlat = 0.0D;
				if((field.get("lonlat") != null) && ! field.get("lonlat").equals("")) {
					String lonlat_parts[] = field.get("lonlat").split(" ");
					actlon = Double.parseDouble(lonlat_parts[0]);
					actlat = Double.parseDouble(lonlat_parts[1]);
					inserthousenumberWithGeometryStmt.setDouble(8, 10.2);
					inserthousenumberWithGeometryStmt.setDouble(9, 50.1);

				}
				//System.out.println("actlon ===" + actlon + "===    actlat ===" + actlat + "===");

				if(! street_idlist.containsKey(actStreet)) {
					selectStreetidStmt.setString(1, actStreet);
					System.out.println("query for street ===" + actStreet + "=== ...");
					ResultSet selstreetRS = selectStreetidStmt.executeQuery();
					if (selstreetRS.next()) {
						street_idlist.put(actStreet, selstreetRS.getInt("id"));
						numberStreetsLoadedDynamically++;
					} else {
						insertstreetstmt.setString(1, actStreet);
						//System.out.println("insert_sql statement ===" + insertstreetsql + "===");
						try {
							ResultSet rs_getautogenkeys = insertstreetstmt.executeQuery();
						    if (rs_getautogenkeys.next()) {
								street_idlist.put(actStreet, rs_getautogenkeys.getInt("id"));
								numberStreetInsertedIntoDB++;
						    } 
						    rs_getautogenkeys.close();
						}
						catch( SQLException e) {
							System.out.println("ERROR: during insert in table evaluation_overview, insert code was ===" + insertstreetsql + "===");
							System.out.println(e.toString());
						}
					}
				}

				if(! previousStreet.equals(actStreet) && ! previousStreet.equals("")) {
					selectOSMStreetGeometryStmt.setInt(1, jobid);
					selectOSMStreetGeometryStmt.setInt(2, street_idlist.get(previousStreet));
					try {
						Integer insertStreetResultParameterIndex = 1;
						ResultSet selectOSMStreetGeometryRS = selectOSMStreetGeometryStmt.executeQuery();
						if(selectOSMStreetGeometryRS.next()) {

							String insertStreetResultParameters = "";
							if (selectOSMStreetGeometryRS.getString("linestring900913") != null) {
								insertStreetResultStmt.setLong(insertStreetResultParameterIndex++, countryid);
								insertStreetResultParameters += "land_id=" + countryid;
								insertStreetResultStmt.setLong(insertStreetResultParameterIndex++, municipalityid);
								insertStreetResultParameters += ", stadt_id=" + municipalityid;
								insertStreetResultStmt.setLong(insertStreetResultParameterIndex++, jobid);
								insertStreetResultParameters += ", job_id=" + jobid;
								insertStreetResultStmt.setString(insertStreetResultParameterIndex++, previousStreet);
								insertStreetResultParameters += ", strasse=" + previousStreet;
								insertStreetResultStmt.setInt(insertStreetResultParameterIndex++, (street_hittype_l + street_hittype_i));
								insertStreetResultParameters += ", hnr_soll=" + (street_hittype_l + street_hittype_i);
								insertStreetResultStmt.setInt(insertStreetResultParameterIndex++, street_hittype_i);
								insertStreetResultParameters += ", hnr_osm=" + street_hittype_i;
								insertStreetResultStmt.setInt(insertStreetResultParameterIndex++, street_hittype_l);
								insertStreetResultParameters += ", hnr_fhlosm=" + street_hittype_l;
								insertStreetResultStmt.setInt(insertStreetResultParameterIndex++, street_hittype_o);
								insertStreetResultParameters += ", hnr_nurosm=" + street_hittype_o;

								Float hausnummernFertigfloat = new Float(street_hittype_i) / new Float(street_hittype_l + street_hittype_i);
								int hausnummernFertigProzent = Math.round(hausnummernFertigfloat * 100);
								
								insertStreetResultStmt.setInt(insertStreetResultParameterIndex++, hausnummernFertigProzent);
								insertStreetResultParameters += ", hnr_abdeck=" + hausnummernFertigProzent;

								insertStreetResultStmt.setString(insertStreetResultParameterIndex++, actStreetMissingHousenumbers.toString());
								insertStreetResultParameters += ", hnr_liste=" + actStreetMissingHousenumbers.toString();
								insertStreetResultStmt.setString(insertStreetResultParameterIndex++, selectOSMStreetGeometryRS.getString("linestring900913"));
								insertStreetResultParameters += ", geometry= ...";
								//System.out.println("insert evaluation result parameters ===" + insertStreetResultParameters + "===,  statement ===" + insertStreetResultSql + "===");
								try {
									insertStreetResultStmt.executeUpdate();
								}
								catch( SQLException e) {
									System.out.println("ERROR: during insert in table exporthrn2shape, insert code was ===" + insertStreetResultSql + "===");
									e.printStackTrace();
								}
							} else {
								System.out.println("WARNUNG: leeres Polygon in jobs_strassen id: " + selectOSMStreetGeometryRS.getLong("id"));
							}
						}
					}
					catch( SQLException e) {
						System.out.println("ERROR: during insert in table evaluation_overview, insert code was ===" + insertstreetsql + "===");
						System.out.println(e.toString());
					}
					actStreetMissingHousenumbers = new StringBuffer();
					street_hittype_i = 0;
					street_hittype_l = 0;
					street_hittype_o = 0;
				}

				if(acthittype.equals("i")) {
					countHousenumbersIdentical++;
					street_hittype_i++;
				} else if(acthittype.equals("l")) {
					countHousenumbersListonly++;
					street_hittype_l++;
					actStreetMissingHousenumbers.append(acthousenumber + " ");
				} else if(acthittype.equals("o")) {
					countHousenumbersOsmonly++;
					street_hittype_o++;
				} else
					System.out.println("ERROR, acthittype unknown ===" + acthittype + "=== in result file lineno " 
						+ (lineindex + 1) + ", fileline content was ===" + actline + "===");

				if(actlon == 0.0D) {
					inserthousenumberWithoutGeometryStmt.setInt(1, countryid);
					inserthousenumberWithoutGeometryStmt.setInt(2, municipalityid);
					inserthousenumberWithoutGeometryStmt.setInt(3, jobid);
					inserthousenumberWithoutGeometryStmt.setString(4,country);
					inserthousenumberWithoutGeometryStmt.setString(5, municipality);
					inserthousenumberWithoutGeometryStmt.setString(6, jobname);
					inserthousenumberWithoutGeometryStmt.setString(7, actStreet);
					inserthousenumberWithoutGeometryStmt.setInt(8, street_idlist.get(actStreet));
					inserthousenumberWithoutGeometryStmt.setString(9, actPostcode);
					inserthousenumberWithoutGeometryStmt.setString(10, acthousenumber); 
					inserthousenumberWithoutGeometryStmt.setString(11, acthousenumbersorted);
					inserthousenumberWithoutGeometryStmt.setString(12, acthittype);
					inserthousenumberWithoutGeometryStmt.setString(13, actosmtype);  
					inserthousenumberWithoutGeometryStmt.setLong(14, actosmid);
					inserthousenumberWithoutGeometryStmt.setString(15, actosmtag);
					//System.out.println("insert_sql statement ===" + inserthousenumberWithoutGeometrySql + "===");
					try {
						inserthousenumberWithoutGeometryStmt.executeUpdate();
					}
					catch( SQLException e) {
						System.out.println("ERROR: during insert in table auswertung_hausnummern, insert code was ===" + inserthousenumberWithoutGeometrySql + "===");
						e.printStackTrace();
					}
				} else {
					inserthousenumberWithGeometryStmt.setInt(1, countryid);
					inserthousenumberWithGeometryStmt.setInt(2, municipalityid);
					inserthousenumberWithGeometryStmt.setInt(3, jobid);
					inserthousenumberWithGeometryStmt.setString(4,country);
					inserthousenumberWithGeometryStmt.setString(5, municipality);
					inserthousenumberWithGeometryStmt.setString(6, jobname);
					inserthousenumberWithGeometryStmt.setString(7, actStreet);
					inserthousenumberWithGeometryStmt.setInt(8, street_idlist.get(actStreet));
					inserthousenumberWithGeometryStmt.setString(9, actPostcode);
					inserthousenumberWithGeometryStmt.setString(10, acthousenumber); 
					inserthousenumberWithGeometryStmt.setString(11, acthousenumbersorted);
					inserthousenumberWithGeometryStmt.setString(12, acthittype);
					inserthousenumberWithGeometryStmt.setString(13, actosmtype);  
					inserthousenumberWithGeometryStmt.setLong(14, actosmid);
					inserthousenumberWithGeometryStmt.setString(15, actosmtag);
					inserthousenumberWithGeometryStmt.setDouble(16, actlon);
					inserthousenumberWithGeometryStmt.setDouble(17, actlat);
					
					//System.out.println("insert_sql statement ===" + inserthousenumberWithGeometrySql + "===");
					try {
						inserthousenumberWithGeometryStmt.executeUpdate();
					}
					catch( SQLException e) {
						System.out.println("ERROR: during insert in table auswertung_hausnummern, insert code was ===" + inserthousenumberWithGeometrySql + "===");
						e.printStackTrace();
					}
				}
			} // end of loop over all content lines
			java.util.Date loopend = new java.util.Date();
			System.out.println("time for loop in sek: " + (loopend.getTime() - loopstart.getTime())/1000);

			selectStreetidStmt.close();
			inserthousenumberWithGeometryStmt.close();
			inserthousenumberWithoutGeometryStmt.close();

			float resultpercentfulfilled = 0.0F;
			if((countHousenumbersListonly  + countHousenumbersIdentical) != 0)
				resultpercentfulfilled = (float) (100.0 * countHousenumbersIdentical / (countHousenumbersListonly  + countHousenumbersIdentical));

			String insertEvaluationResultSql = "INSERT INTO evaluations (land_id, stadt_id, job_id,"
				+ " number_target, number_identical, number_osmonly,"
				+ " tstamp, osmdb_tstamp)"
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?);";
			PreparedStatement insertEvaluationResultStmt = con_hausnummern.prepareStatement(insertEvaluationResultSql);

				// store result of actual job evaluation in evaluations overview table, including timestamps for evalation time and time of local osm db 
			insertEvaluationResultStmt.setInt(1, countryid);
			insertEvaluationResultStmt.setInt(2, municipalityid);
			insertEvaluationResultStmt.setInt(3, jobid);
			insertEvaluationResultStmt.setInt(4, (countHousenumbersListonly + countHousenumbersIdentical));
			insertEvaluationResultStmt.setInt(5, countHousenumbersIdentical);
			insertEvaluationResultStmt.setInt(6, countHousenumbersOsmonly);
			insertEvaluationResultStmt.setTimestamp(7, new Timestamp(evaluationtime.getTime()), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
			insertEvaluationResultStmt.setTimestamp(8, new Timestamp(osmtime.getTime()), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
			//System.out.println("insert evaluation result statement ===" + insertEvaluationResultSql + "===");
			System.out.println("Info: store evaluation result in table evaluations ...");
			try {
				java.util.Date time_beforeinsert = new java.util.Date();
				insertEvaluationResultStmt.executeUpdate();
				java.util.Date time_afterinsert = new java.util.Date();
				System.out.println("TIME for insert evaluation result DB action in ms. "+(time_afterinsert.getTime()-time_beforeinsert.getTime()));
			}
			catch( SQLException e) {
				System.out.println("ERROR: during insert in table osmhausnummern_hausnummern identische, insert code was ===" + insertEvaluationResultSql + "===");
				e.printStackTrace();
			}
			insertEvaluationResultStmt.close();

			String EvaluationResultMapSql = "SELECT id FROM exportjobs2shape"
				+ " WHERE land_id = ? AND stadt_id = ? AND job_id = ?;";
			PreparedStatement selectEvaluationResultMapStmt = con_hausnummern.prepareStatement(EvaluationResultMapSql);
			System.out.println("Info: store evaluation result in table exportjobs2shape ...");
			
				// store result of actual job evaluation in evaluations overview table for Map, including timestamps for evalation time and time of local osm db 
			try {
				selectEvaluationResultMapStmt.setInt(1, countryid);
				selectEvaluationResultMapStmt.setInt(2, municipalityid);
				selectEvaluationResultMapStmt.setInt(3, jobid);
				//System.out.println("select evaluation result map statement ===" + EvaluationResultMapSql + "===");
				java.util.Date time_beforeinsert = new java.util.Date();
				ResultSet selectEvaluationResultMapRs = selectEvaluationResultMapStmt.executeQuery();
				if(selectEvaluationResultMapRs.next()) {
					EvaluationResultMapSql = "UPDATE exportjobs2shape"
						+ " SET stadtbezrk = ?, hnr_soll = ?, hnr_osm = ?,"
						+ " hnr_fhlosm = ?, hnr_nurosm = ?, hnr_abdeck = ?,"
						+ " polygon = ST_Transform(ST_Geomfromtext(?, ?), 4326),"
						+ " timestamp = now()"
						+ " WHERE land_id = ? AND stadt_id = ? AND job_id = ?;";
					PreparedStatement updateEvaluationResultMapStmt = con_hausnummern.prepareStatement(EvaluationResultMapSql);
					updateEvaluationResultMapStmt.setString(1, jobname);
					updateEvaluationResultMapStmt.setInt(2, (countHousenumbersListonly + countHousenumbersIdentical));
					updateEvaluationResultMapStmt.setInt(3, countHousenumbersIdentical);
					updateEvaluationResultMapStmt.setInt(4, countHousenumbersListonly);
					updateEvaluationResultMapStmt.setInt(5, countHousenumbersOsmonly);
					updateEvaluationResultMapStmt.setInt(6, (int)(Math.round(resultpercentfulfilled * 10.0) / 10.0));
					updateEvaluationResultMapStmt.setString(7, polygonAsText);
					updateEvaluationResultMapStmt.setInt(8, polygonSrid);
					updateEvaluationResultMapStmt.setInt(9, countryid);
					updateEvaluationResultMapStmt.setInt(10, municipalityid);
					updateEvaluationResultMapStmt.setInt(11, jobid);
					updateEvaluationResultMapStmt.executeUpdate();
					updateEvaluationResultMapStmt.close();
				} else {
					EvaluationResultMapSql = "INSERT INTO exportjobs2shape"
						+ " (land_id, stadt_id, job_id, stadtbezrk, hnr_soll, hnr_osm,"
						+ " hnr_fhlosm, hnr_nurosm, timestamp, hnr_abdeck,polygon)"
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ST_Transform(ST_Geomfromtext(?, ?), 4326));";
					PreparedStatement insertEvaluationResultMapStmt = con_hausnummern.prepareStatement(EvaluationResultMapSql);
					insertEvaluationResultMapStmt.setInt(1, countryid);
					insertEvaluationResultMapStmt.setInt(2, municipalityid);
					insertEvaluationResultMapStmt.setInt(3, jobid);
					insertEvaluationResultMapStmt.setString(4, jobname);
					insertEvaluationResultMapStmt.setInt(5, (countHousenumbersListonly + countHousenumbersIdentical));
					insertEvaluationResultMapStmt.setInt(6, countHousenumbersIdentical);
					insertEvaluationResultMapStmt.setInt(7, countHousenumbersListonly);
					insertEvaluationResultMapStmt.setInt(8, countHousenumbersOsmonly);
					insertEvaluationResultMapStmt.setInt(9, (int)(Math.round(resultpercentfulfilled * 10.0) / 10.0));
					insertEvaluationResultMapStmt.setString(10, polygonAsText);
					insertEvaluationResultMapStmt.setInt(11, polygonSrid);
					insertEvaluationResultMapStmt.executeUpdate();
					insertEvaluationResultMapStmt.close();
				}
				selectEvaluationResultMapStmt.close();
				java.util.Date time_afterinsert = new java.util.Date();
				System.out.println("TIME for insert or update of evaluation result map DB action in ms. "+(time_afterinsert.getTime()-time_beforeinsert.getTime()));
			}
			catch( SQLException e) {
				System.out.println("ERROR: during select result overview map entry, select code was ===" + EvaluationResultMapSql + "===");
				e.printStackTrace();
			}

				// if job is from jobqueue table, then update state of job
			if(!serverobjectId.equals("")) {
				System.out.println("after response stream closed now work on available serverobjectId ===" + serverobjectId + "===");
				if(serverobjectId.indexOf("jobqueue:") == 0) {
					String serverobjectId_parts[] = serverobjectId.split(":");
					if(serverobjectId_parts.length == 2) {
						String updateJobqueueSql = "UPDATE jobqueue set state = 'finished', finishedtime = now()";
						updateJobqueueSql += " WHERE";
						updateJobqueueSql += " id = ? AND";
						updateJobqueueSql += " state = 'uploaded';";
						updateJobqueueSql += ";";
						PreparedStatement updateJobqueueStmt = con_hausnummern.prepareStatement(updateJobqueueSql);
						updateJobqueueStmt.setLong(1, Long.parseLong(serverobjectId_parts[1]));
						updateJobqueueStmt.executeUpdate();
					} else {
						System.out.println("Error in getHousenumberlist: unknown structure in Serverobjectid, id complete ===" + serverobjectId + "===, will be ignored");
					}
				} else {
					System.out.println("Warning in getHousenumberlist: unknown serverobjectId Prefix, id complete ===" + serverobjectId + "===, will be ignored");
				}
				System.out.println("after response end of work on available serverobjectId ===" + serverobjectId + "===");
			}
			
			
			
			System.out.println("Info: start transaction commit ...");
			java.util.Date commitstart = new java.util.Date();
			con_hausnummern.commit();
			java.util.Date commitend = new java.util.Date();
			System.out.println("time for commit in sek: " + (commitend.getTime() - commitstart.getTime())/1000);
			System.out.println("Info: finished transaction commit ...");

			selectqueryStmt.close();

			con_hausnummern.close();

			System.out.println("loaded streets from official housenumberlist: " + numberStreetsFromOfficialList);
			System.out.println("loaded streets dynamically: " + numberStreetsLoadedDynamically);
			System.out.println("inserted new streets into DB: " + numberStreetInsertedIntoDB);

		} // end of try to connect to DB and operate with DB
		catch(ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch( SQLException e) {
			e.printStackTrace();
			try {
				con_hausnummern.close();
			} catch( SQLException innere) {
				System.out.println("inner sql-exception (tried to to close connection ...");
				innere.printStackTrace();
			}
			return false;
		}
		System.out.println("time for insertResultIntoDBouter in sek: " + (new Date().getTime() - insertResultIntoDBouter.getTime())/1000);
		return true;
	}


	public static void main(String args[]) {
//TODO entry to configuration file
		String resultfiles_rootpath = configuration.upload_homedir;

		String resultfiles_uploadpath = resultfiles_rootpath  + File.separator + "open";
		String resultfiles_importedpath = resultfiles_rootpath  + File.separator + "imported";

		for (int lfdnr = 0; lfdnr < args.length; lfdnr++) {
			System.out.println("args[" + lfdnr + "] ===" + args[lfdnr] + "===");
		}
		if ((args.length >= 1) && (args[0].equals("-h"))) {
			System.out.println("-opendir directory");
			System.out.println("-importeddir directory");
			return;
		}

		if (args.length >= 1) {
			int argsOkCount = 0;
			for (int argsi = 0; argsi < args.length; argsi += 2) {
				System.out.print(" args pair analysing #: " + argsi + "  ===" + args[argsi] + "===");
				if (args.length > argsi + 1) {
					System.out.println("  args # + 1: " + (argsi + 1) + "   ===" + args[argsi + 1] + "===");
				}
				if (args[argsi].equals("-opendir")) {
					resultfiles_uploadpath = args[argsi + 1];
					argsOkCount  += 2;
				}
				if (args[argsi].equals("-importeddir")) {
					resultfiles_importedpath = args[argsi + 1];
					argsOkCount  += 2;
				}
			}
			if (argsOkCount < args.length) {
				System.out.println("ERROR: not all programm parameters were valid, STOP");
				return;
			}
		}
		
		DateFormat dateformat = DateFormat.getDateTimeInstance();

		
		String filename = "";
		String importworkPathandFilename = resultfiles_uploadpath + File.separator + "batchimport.active";
		String importworkoutputline = "";
		try {
			File importworkPathandFilenameHandle = new File(importworkPathandFilename);
			if(importworkPathandFilenameHandle.exists() && !importworkPathandFilenameHandle.isDirectory()) {
				System.out.println("Batchimport already active, stopp processign of this program");
				return;
			}
			PrintWriter workprogressOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(importworkPathandFilename, true),StandardCharsets.UTF_8)));
			workprogressOutput.println("Start of batchimport: " + dateformat.format(new Date()));
			workprogressOutput.close();

			File dir_filestructure = new File(resultfiles_uploadpath);

			String[] dirrelative_stringarray = dir_filestructure.list();
			System.out.println("Number of entries in directory: "+dirrelative_stringarray.length+" in Directory ==="+resultfiles_uploadpath+"===");
			for(Integer diri=0;diri<dirrelative_stringarray.length;diri++) {
				String actualFilename = dirrelative_stringarray[diri];
				String actual_entry = resultfiles_uploadpath + File.separator + actualFilename;
				File actual_filehandle = new File(actual_entry);
				System.out.println("actual file entry ===" + actualFilename + "=== in directory ==="+resultfiles_uploadpath+"===   complete path ==="+actual_entry+"===");
				if(actual_filehandle.isDirectory()) {
					System.out.println("actual file entry is DIRECTORY ===" + actualFilename 
						+ "=== Sub directory " + actual_entry + " will be ignored within directory ===" + resultfiles_uploadpath);
				}
				else if (actual_filehandle.isFile()) {
					if(actualFilename.indexOf(".") != -1) {
						String actual_fileextension = actualFilename.substring(actualFilename.lastIndexOf(".")+1);
						System.out.println("actual file entry has fileextension ==="+actual_fileextension+"===");
						if(actual_fileextension.equals("result")) {
							System.out.println("actual file entry is FILE and .result ==="+actualFilename+"=== (subcall starts...) in directory ===" 
								+ resultfiles_uploadpath + "===   complete path ===" + resultfiles_uploadpath + File.separator
								+ actualFilename.substring(0,actualFilename.lastIndexOf("."))+"===");
							importworkoutputline = actualFilename + "\t" + dateformat.format(new Date()) + "\t";

							BufferedReader filereader = new BufferedReader(new InputStreamReader(new FileInputStream(actual_filehandle.getAbsoluteFile()),StandardCharsets.UTF_8));
							String fileline = "";
							StringBuffer filecontent = new StringBuffer();
							while ((fileline = filereader.readLine()) != null) {
								filecontent.append(fileline + "\n");
							}
							filereader.close();
							
							String content = filecontent.toString();

							boolean imported = insertResultIntoDB(content);
							if(imported) {
								String destinationFilename = resultfiles_importedpath + File.separator + actualFilename;
								File destinationFilenameHandle = new File(destinationFilename);
								actual_filehandle.renameTo(destinationFilenameHandle);
								importworkoutputline += dateformat.format(new Date()) + "\t" + "successful";
							} else {
								importworkoutputline += dateformat.format(new Date()) + "\t" + "failed";
							}
						}
					}
					workprogressOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
							new FileOutputStream(importworkPathandFilename, true),StandardCharsets.UTF_8)));
					workprogressOutput.println(importworkoutputline);
					workprogressOutput.close();

				}
			} // end of for-loop over actual path
			if(importworkPathandFilenameHandle.exists() && !importworkPathandFilenameHandle.isDirectory()) {
				String destinationworkPathandFilename = resultfiles_uploadpath + File.separator + "batchimport.finished";
				File destinationworkPathandFilenameHandle = new File(destinationworkPathandFilename);
				importworkPathandFilenameHandle.renameTo(destinationworkPathandFilenameHandle);
				System.out.println("Batchimport progress file renamed to finish-state");
			}
		} catch (IOException ioerror) {
			System.out.println("ERORR: IOException happened, details follows ...");
			System.out.println(" .. couldn't open file to write, filename was ===" + filename + "===");
			System.out.println(" .. couldn't open file to write, filename was ===" + ioerror.toString() + "===");
			ioerror.printStackTrace();
		}
	}
}

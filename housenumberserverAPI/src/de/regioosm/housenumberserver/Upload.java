package de.regioosm.housenumberserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import net.balusc.http.multipart.MultipartMap;

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
	// the url, when this servlet class will be executed
@WebServlet("/Upload")
	//	parameters for external class, which interpret the client request:
	//	location: to which temporary directory, a file can be created and stored.
	//	maxFileSize: up to which size, the upload file can be. CAUTION: as of 2015-01-11, the upload file is not compressed
@MultipartConfig(location = "/tmp", maxFileSize = 20971520) // 20MB.	
public class Upload extends HttpServlet {
	private static final long serialVersionUID = 1L;
		// load content of configuration file, which contains filesystem entries and database connection details
	static Applicationconfiguration configuration = new Applicationconfiguration();
	static Connection con_hausnummern;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public Upload() {
        super();
        // TODO Auto-generated constructor stub
    }


    /**
     * normalize housenumber to 4 digits, filling some prefix 0 (zeros), to enable sorting of housenumbers, even with literal suffixes
     * <p>
     * @param hausnummer: 	housenumber, optionally with numeric or literal suffixes
     * @return:	housenumber in normalized form, means with leading 0 (zeros). Housenumber "3a" will be returned to "0003a", "157 1/2a" to "0157 1/2a"
     */
    private String setHausnummerNormalisiert(String hausnummer) {
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
    private String insertResultIntoDB(String content, String country, String municipality) {
		java.util.Date insertResultIntoDBouter = new java.util.Date();
		java.util.Date insertResultIntoDBinner = new java.util.Date();

		if(content.length() == 0) {
			System.out.println("leerer Content, Abbruch");
			return "leer Content, Abbruch";
		}

		
		try {
			Class.forName("org.postgresql.Driver");
	
			String url_hausnummern = configuration.db_application_url;
			con_hausnummern = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);

			System.out.println("beginn von insertfkt");
			
			String select_sql = "SELECT land.id AS countryid, stadt.id AS municipalityid, jobs.id AS jobid"
				+ " FROM land, stadt, gebiete, jobs WHERE"
				+ " land = ?"
				+ " AND stadt = ?"
				+ " AND gebiete.stadt_id = stadt.id"
				+ " AND jobs.gebiete_id = gebiete.id"
				+ " AND stadt = jobname"
				+ " ORDER BY admin_level;";

			PreparedStatement selectqueryStmt = con_hausnummern.prepareStatement(select_sql);
			selectqueryStmt.setString(1, country);
			selectqueryStmt.setString(2, municipality);
			ResultSet existingmunicipalityRS = selectqueryStmt.executeQuery();

			
			String deletelastevaluationSql = "DELETE FROM auswertung_hausnummern"
					+ " WHERE land_id = ? AND stadt_id = ? AND job_id = ?;";
			PreparedStatement deletelastevaluationStmt = con_hausnummern.prepareStatement(deletelastevaluationSql);
			
			String selectstreetid_sql = "SELECT id FROM strasse where strasse = ?;";
			PreparedStatement selectstreetidstmt = con_hausnummern.prepareStatement(selectstreetid_sql);

			String selectallofficialstreetsSql = "SELECT DISTINCT ON (strasse) strasse, strasse.id AS id"
				+ " FROM stadt_hausnummern, strasse, stadt, land"
				+ " WHERE stadt_hausnummern.strasse_id = strasse.id"
				+ " AND stadt_hausnummern.stadt_id = stadt.id"
				+ " AND stadt_hausnummern.land_id = land.id"
				+ " AND stadt = ? AND land = ?;";
			PreparedStatement selectallofficialstreetsStmt = con_hausnummern.prepareStatement(selectallofficialstreetsSql);
			
			String insertstreetsql = "INSERT INTO strasse (strasse) VALUES (?) returning id;";
			PreparedStatement insertstreetstmt = con_hausnummern.prepareStatement(insertstreetsql);
			
			String inserthousenumberWithGeometrySql = "INSERT INTO auswertung_hausnummern"
					+ " (land_id, stadt_id, job_id, copyland, copystadt, copystrasse,"
					+ " strasse_id, hausnummer, hausnummer_sortierbar, treffertyp,"
					+ " osm_objektart, osm_id, objektart, point"
					+ ")"
					+ " VALUES(?, ?, ?, ?, ?, ?,"
					+ " ?, ?, ?, ?,"
					+ " ?, ?, ?, ST_Setsrid(ST_Makepoint(?, ?), 4326));";
			PreparedStatement inserthousenumberWithGeometryStmt = con_hausnummern.prepareStatement(inserthousenumberWithGeometrySql);

			String inserthousenumberWithoutGeometrySql = "INSERT INTO auswertung_hausnummern"
					+ " (land_id, stadt_id, job_id, copyland, copystadt, copystrasse,"
					+ " strasse_id, hausnummer, hausnummer_sortierbar, treffertyp,"
					+ " osm_objektart, osm_id, objektart"
					+ ")"
					+ " VALUES(?, ?, ?, ?, ?, ?,"
					+ " ?, ?, ?, ?,"
					+ " ?, ?, ?);";
			PreparedStatement inserthousenumberWithoutGeometryStmt = con_hausnummern.prepareStatement(inserthousenumberWithoutGeometrySql);


			int countryid = 0;
			int municipalityid = 0;
			int jobid = 0;

					//TODO check and code, what happens with more than one row
			if (existingmunicipalityRS.next()) {
				countryid = existingmunicipalityRS.getInt("countryid");
				municipalityid = existingmunicipalityRS.getInt("municipalityid");
				jobid = existingmunicipalityRS.getInt("jobid");
			} else {
				return "Error: There is no matching municipality '" + municipality + "' within the country '" + country + "' in the server side housenumber database. The uploaded result will be suspended";
			}
			

				// delete up to now active evaluation for same municipality
				// It will be checked against jobname = municipality name to only delete complete municipality evaluation, 
				// not optionally available subadmin evaluations
			System.out.println("deleteauswertunghausnummern statement ===" + deletelastevaluationSql + "===");
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

				// storage of streetnames and their internal DB id. If a street is missing in DB, it will be inserted,
				// before the insert of the housenumbers at the streets will be inserted
			HashMap<String, Integer> street_idlist = new HashMap<String, Integer>();

			int numberStreetsFromOfficialList = 0;
			int numberStreetsLoadedDynamically = 0;
			int numberStreetInsertedIntoDB = 0;
			try {
				selectallofficialstreetsStmt.setString(1, municipality);
				selectallofficialstreetsStmt.setString(2, country);
				ResultSet selectallofficialstreetsRS = selectallofficialstreetsStmt.executeQuery();
				while(selectallofficialstreetsRS.next()) {
					street_idlist.put(selectallofficialstreetsRS.getString("strasse"), selectallofficialstreetsRS.getInt("id"));
					numberStreetsFromOfficialList++;
				}
			}
			catch( SQLException e) {
				System.out.println("ERROR, when tried to get all official street names from municipality ===" + municipality + "=== in country ===" + country + "===");
				e.printStackTrace();
			}


				// change to transaction mode
			con_hausnummern.setAutoCommit(false);

				// store column names, below found in file header line
			HashMap<Integer, String> headerfield = new HashMap<Integer, String>();
			String lines[] = content.split("\n");

				// loop over all result file lines
			java.util.Date loopstart = new java.util.Date();
			for(int lineindex = 0; lineindex < lines.length; lineindex++) {
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
						System.out.println("stored headerfield[" + colindex + "] ===" + actcolumn + "===");
					}
					continue;
				}
					// ignore other comment lines
				if(actline.indexOf("#") == 0)
					continue;
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
					System.out.println("stored field[" + headerfield.get(colindex) + "] ===" + actcolumn + "===");
				}

				String actstreet = "";
				if(field.get("strasse") != null)
					actstreet = field.get("strasse");
				System.out.println("actstreet ===" + actstreet + "===");

				String acthousenumber = "";
				if(field.get("hausnummer") != null)
					acthousenumber = field.get("hausnummer");
				System.out.println("acthousenumber ===" + acthousenumber + "===");
				String acthousenumbersorted = setHausnummerNormalisiert(acthousenumber);
				System.out.println("acthousenumbersorted ===" + acthousenumbersorted + "===");

				String acthittype = "";
				if(field.get("treffertyp") != null)
					acthittype = field.get("treffertyp");
				System.out.println("acthittype ===" + acthittype + "===");

				String actosmtag = "";	// OSM "key"=>"value" of the osm object, which holds the housenumber
				if(field.get("osmtag") != null)
					actosmtag = field.get("osmtag");
				System.out.println("actosmtag ===" + actosmtag + "===");

				String actosmtype = "";	// will be "node", "way" or "relation"
				if(field.get("osmtyp") != null)
					actosmtype = field.get("osmtyp");
				System.out.println("actosmtype ===" + actosmtype + "===");

				Long actosmid = -1L;	// OSM id of the osm objects, which holds the housenumber. It must be used in conjunction with actosmtype
				if(field.get("osmid") != null)
					actosmid = Long.parseLong(field.get("osmid"));
				System.out.println("actosmid ===" + actosmid + "===");

				double actlon = 0.0D;
				double actlat = 0.0D;
				if((field.get("lonlat") != null) && ! field.get("lonlat").equals("")) {
					String lonlat_parts[] = field.get("lonlat").split(" ");
					actlon = Double.parseDouble(lonlat_parts[0]);
					actlat = Double.parseDouble(lonlat_parts[1]);
					inserthousenumberWithGeometryStmt.setDouble(8, 10.2);
					inserthousenumberWithGeometryStmt.setDouble(9, 50.1);

				}
				System.out.println("actlon ===" + actlon + "===    actlat ===" + actlat + "===");

				if(! street_idlist.containsKey(actstreet)) {
					selectstreetidstmt.setString(1, actstreet);
					System.out.println("query for street ===" + actstreet + "=== ...");
					ResultSet selstreetRS = selectstreetidstmt.executeQuery();
					if (selstreetRS.next()) {
						street_idlist.put(actstreet, selstreetRS.getInt("id"));
						numberStreetsLoadedDynamically++;
					} else {
						insertstreetstmt.setString(1, actstreet);
						System.out.println("insert_sql statement ===" + insertstreetsql + "===");
						try {
							ResultSet rs_getautogenkeys = insertstreetstmt.executeQuery();
						    if (rs_getautogenkeys.next()) {
								street_idlist.put(actstreet, rs_getautogenkeys.getInt("id"));
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
				if(actlon == 0.0D) {
					inserthousenumberWithoutGeometryStmt.setInt(1, countryid);
					inserthousenumberWithoutGeometryStmt.setInt(2, municipalityid);
					inserthousenumberWithoutGeometryStmt.setInt(3, jobid);
					inserthousenumberWithoutGeometryStmt.setString(4,country);
					inserthousenumberWithoutGeometryStmt.setString(5, municipality);
					inserthousenumberWithoutGeometryStmt.setString(6, actstreet);
					inserthousenumberWithoutGeometryStmt.setInt(7, street_idlist.get(actstreet));
					inserthousenumberWithoutGeometryStmt.setString(8, acthousenumber); 
					inserthousenumberWithoutGeometryStmt.setString(9, acthousenumbersorted);
					inserthousenumberWithoutGeometryStmt.setString(10, acthittype);
					inserthousenumberWithoutGeometryStmt.setString(11, actosmtype);  
					inserthousenumberWithoutGeometryStmt.setLong(12, actosmid);
					inserthousenumberWithoutGeometryStmt.setString(13, actosmtag);
					System.out.println("insert_sql statement ===" + inserthousenumberWithoutGeometrySql + "===");
					try {
						java.util.Date time_beforeinsert = new java.util.Date();
						inserthousenumberWithoutGeometryStmt.executeUpdate();
						java.util.Date time_afterinsert = new java.util.Date();
						System.out.println("TIME for insert record osmhausnummern_hausnummern " + actstreet + " " + acthousenumber + " " + acthittype
							+ ", in ms. "+(time_afterinsert.getTime()-time_beforeinsert.getTime()));
					}
					catch( SQLException e) {
						System.out.println("ERROR: during insert in table osmhausnummern_hausnummern identische, insert code was ===" + inserthousenumberWithoutGeometrySql + "===");
						e.printStackTrace();
					}
				} else {
					inserthousenumberWithGeometryStmt.setInt(1, countryid);
					inserthousenumberWithGeometryStmt.setInt(2, municipalityid);
					inserthousenumberWithGeometryStmt.setInt(3, jobid);
					inserthousenumberWithGeometryStmt.setString(4,country);
					inserthousenumberWithGeometryStmt.setString(5, municipality);
					inserthousenumberWithGeometryStmt.setString(6, actstreet);
					inserthousenumberWithGeometryStmt.setInt(7, street_idlist.get(actstreet));
					inserthousenumberWithGeometryStmt.setString(8, acthousenumber); 
					inserthousenumberWithGeometryStmt.setString(9, acthousenumbersorted);
					inserthousenumberWithGeometryStmt.setString(10, acthittype);
					inserthousenumberWithGeometryStmt.setString(11, actosmtype);  
					inserthousenumberWithGeometryStmt.setLong(12, actosmid);
					inserthousenumberWithGeometryStmt.setString(13, actosmtag);
					inserthousenumberWithGeometryStmt.setDouble(14, actlon);
					inserthousenumberWithGeometryStmt.setDouble(15, actlat);
					
					System.out.println("insert_sql statement ===" + inserthousenumberWithGeometrySql + "===");
					try {
						java.util.Date time_beforeinsert = new java.util.Date();
						inserthousenumberWithGeometryStmt.executeUpdate();
						java.util.Date time_afterinsert = new java.util.Date();
						System.out.println("TIME for insert record osmhausnummern_hausnummern " + actstreet + " " + acthousenumber + " " + acthittype
							+ ", in ms. "+(time_afterinsert.getTime()-time_beforeinsert.getTime()));
					}
					catch( SQLException e) {
						System.out.println("ERROR: during insert in table osmhausnummern_hausnummern identische, insert code was ===" + inserthousenumberWithGeometrySql + "===");
						e.printStackTrace();
					}
				}

			} // end of loop over all content lines
			java.util.Date loopend = new java.util.Date();
			System.out.println("time for loop in sek: " + (loopend.getTime() - loopstart.getTime())/1000);

			java.util.Date commitstart = new java.util.Date();
			con_hausnummern.commit();
			java.util.Date commitend = new java.util.Date();
			System.out.println("time for commit in sek: " + (commitend.getTime() - commitstart.getTime())/1000);

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
			return e.toString();
		}
		System.out.println("time for insertResultIntoDBouter in sek: " + (new Date().getTime() - insertResultIntoDBouter.getTime())/1000);
		return "ganz am ende der sub-fkt. unerwartet";
	}
    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter writer = response.getWriter();
		
		writer.println("<html>");
		writer.println("<head><title>doGet aktiv</title></head>");
		writer.println("<body>");
		writer.println("	<h1>Hello World und Otto from a Servlet in Upload.java!</h1>");
		writer.println("<body>");
		writer.println("</html>");
			
		writer.close();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String filename = "";
		try {
			System.out.println("request kompletto ===" + request.toString() + "===");
			System.out.println("ok, in doPost angekommen ...");
			MultipartMap map = new MultipartMap(request, this);

			System.out.println("nach multipartmap in doPost ...");

			String country = map.getParameter("country");
			String municipality = map.getParameter("municipality");
			File file = map.getFile("result");
			System.out.println("temporary file ===" + file.getAbsoluteFile() + "===");

			BufferedReader filereader = new BufferedReader(new InputStreamReader(new FileInputStream(file.getAbsoluteFile()),StandardCharsets.UTF_8));
		    String fileline = "";
		    StringBuffer filecontent = new StringBuffer();
		    while ((fileline = filereader.readLine()) != null) {
		    	filecontent.append(fileline + "\n");
			}
			filereader.close();

			String content = filecontent.toString();

			// Now do your thing with the obtained input.
			System.out.println(" country ===" + country + "===");
			System.out.println(" municipality ===" + municipality + "===");
			System.out.println(" content length ===" + content.length() + "===");

			PrintWriter writer = response.getWriter();

			response.setContentType("text/html; charset=utf-8");

			writer.println("<html>");
			writer.println("<head><meta charset=\"utf-8\"><title>doPost aktiv</title></head>");
			writer.println("<body>");
			writer.println("	<h1>Upload.java!</h1>");


			DateFormat time_formatter = new SimpleDateFormat("yyyyMMdd-HHmmssZ");
			String uploadtime = time_formatter.format(new Date());

			filename += "/home/openstreetmap/temp/uploaddata/open";
			filename += "/" + uploadtime + ".result";
			System.out.println("uploaddatei ===" + filename + "===");

			File outputfile = new File(filename);
			outputfile.createNewFile();

			System.out.println("Filerechte vor setzen ...");
			System.out.println("Is Execute allow : " + outputfile.canExecute());
			System.out.println("Is Write allow : " + outputfile.canWrite());
			System.out.println("Is Read allow : " + outputfile.canRead());

			outputfile.setReadable(true);
			outputfile.setWritable(true);
			outputfile.setExecutable(false);

			File outputfile2 = new File(filename);
			System.out.println("Filerechte nach setzen ...");
			System.out.println("Is Execute allow : " + outputfile2.canExecute());
			System.out.println("Is Write allow : " + outputfile2.canWrite());
			System.out.println("Is Read allow : " + outputfile2.canRead());

			PrintWriter uploadOutput = null;
			uploadOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filename),StandardCharsets.UTF_8)));
			uploadOutput.println(content);
			uploadOutput.close();


			writer.println(" country ===" + country + "===");
			writer.println(" municipality ===" + municipality + "===");
			writer.println(" content length ===" + content.length() + "===");

			writer.println("<body>");
			writer.println("</html>");
				
			writer.close();

			System.out.println("request.getCharacterEncoding() ===" + request.getCharacterEncoding() + "===");
			System.out.println(" country ===" + country + "===");
			System.out.println(" municipality ===" + municipality + "===");

			System.out.println(" Sub-fkt insertResultIntoDB wird aufgerufen ...");
			//writer.println("output von sub-fkt <<<");
			System.out.println(insertResultIntoDB(content, country, municipality));
			//writer.println(">>> output von sub-fkt");
			System.out.println(" Sub-fkt insertResultIntoDB ist fertig");
		} catch (IOException ioerror) {
			System.out.println("ERORR: IOException happened, details follows ...");
			System.out.println(" .. couldn't open file to write, filename was ===" + filename + "===");
			System.out.println(" .. couldn't open file to write, filename was ===" + ioerror.toString() + "===");
			ioerror.printStackTrace();
		} catch (ServletException se) {
			System.out.println("ServletException happened, details follows ...");
			System.out.println("  .. details ===" + se.toString() + "===");
			se.printStackTrace();
		}
	}
}

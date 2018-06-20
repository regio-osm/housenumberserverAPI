package de.regioosm.housenumberserverAPI;

/*
 *
 *  overpass api anfrage zu benamten Straßen von Augsburg
[timeout:3600][maxsize:1073741824]
[out:xml];

area(3600062407)->.boundaryarea;


(
  way(area.boundaryarea)["highway"]["name"];>;
);
// Ergebnis ausgeben samt metadaten
out meta;
 */


/*
 * 
offzielle hausnummern im Stadtbezirk gemäß dort vorkommender Straßen (nach jobs_strassen) inkl. zuvieler Hausnummern weil Straße auch teilweise außerhalb Stadtbezirk weitergeht

select strasse,hausnummer from stadt_hausnummern as sh,jobs_strassen as js, jobs as j, strasse as str  where sh.stadt_id = j.stadt_id and js.job_id = j.id and sh.strasse_id = str.id and js.strasse_id = str.id and jobname = 'Bezirksteil Alte Kaserne' order by strasse,hausnummer_sortierbar;


wie vor, nur jetzt dei in osm vorhandenen Hausnummern (aber teilweise auch noch in benachbarten Stadtbezirken)

select distinct on (strasse, hausnummer_sortierbar) strasse, hausnummer,st_x(point),st_y(point) from auswertung_hausnummern as ah, strasse as str where copystadt = 'München' and treffertyp = 'i' and ah.strasse_id = str.id and strasse_id in (select strasse_id from jobs_strassen as js, jobs as j where js.job_id = j.id and jobname = 'Bezirksteil Alte Kaserne') order by strasse, hausnummer_sortierbar;


die in osm vorhandenen Hausnummern in zutreffenden Straßen, die aber per Koordinate außerhalb des Stadtbezirks sind

select distinct on (strasse, hausnummer_sortierbar) strasse, hausnummer,st_x(point),st_y(point) from auswertung_hausnummern as ah, strasse as str where copystadt = 'München' and treffertyp = 'i' and ah.strasse_id = str.id and strasse_id in (select strasse_id from jobs_strassen as js, jobs as j where js.job_id = j.id and jobname = 'Bezirksteil Alte Kaserne')  and not st_within(point,(select st_transform(polygon,4326) from gebiete where name = 'Bezirksteil Alte Kaserne'))  order by strasse, hausnummer_sortierbar;


die passende abfrage müsste dann sein


select strasse,hausnummer from stadt_hausnummern as sh,jobs_strassen as js, jobs as j, strasse as str  where sh.stadt_id = j.stadt_id and js.job_id = j.id and sh.strasse_id = str.id and js.strasse_id = str.id and jobname = 'Bezirksteil Alte Kaserne' and (strasse,hausnummer_sortierbar) not in (select distinct on (strasse, hausnummer_sortierbar) strasse, hausnummer_sortierbar from auswertung_hausnummern as ah, strasse as str where copystadt = 'München' and treffertyp = 'i' and ah.strasse_id = str.id and strasse_id in (select strasse_id from jobs_strassen as js, jobs as j where js.job_id = j.id and jobname = 'Bezirksteil Alte Kaserne')  and not st_within(point,(select st_transform(polygon,4326) from gebiete where name = 'Bezirksteil Alte Kaserne'))) order by strasse,hausnummer_sortierbar;

im Detail noch 1 Hausnummer Unterschied, diese neue Auswertung offenbar falsch
http://www.openstreetmap.org/?node=1317214798
Gebäude ist schwerpunktmäßig außerhalb, aber der Eingang (node) hat die Adresse und ist im Stadtbezirk

Ursache: es gibt einen Haupteingang (der o.g. OSM node) mit der Adresse und das Gebäude hat auch die Adresse, davon wurde wohl der Schwerpunkt ermittelt und der ist außerhalb
 
Fehler 13.02.2015:
* http://www.openstreetmap.org/?node=312391018
	enthält Hausnummer 3-5: 3 fehlt komplett und 5 wird als nur-osm angezeigt
	vermutlich gibt es die Rosenstraße nochmal in München und wird außerhalb des Stadtbezirks 01 vorgefunden und deshalb gefiltert?
* http://www.openstreetmap.org/?way=108235151  (Müllerstraße 56)
	ein kleiner Teil des Hauses ist im Bezirk, der Schwerpunkt aber außerhalb


*/


import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

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

	// the url, when this servlet class will be executed
@WebServlet("/getHousenumberlist")
public class getHousenumberlist extends HttpServlet {
	private static final long serialVersionUID = 1L;
		// load content of configuration file, which contains filesystem entries and database connection details
	static Applicationconfiguration configuration;
	static Connection housenumberConn;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public getHousenumberlist() {
        super();
        // TODO Auto-generated constructor stub
    }

    
    /**
     * initialization on servlett startup
     */
    public void 	init(ServletConfig config) {
    	System.out.println("\n\nok, servlet v20180408 " + config.getServletName() + " will be initialized now ...\n");

		String path = config.getServletContext().getRealPath("/WEB-INF");
		configuration = new Applicationconfiguration(path);

		try {
			Class.forName("org.postgresql.Driver");
			
			String url_hausnummern = configuration.db_application_url;
			housenumberConn = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);
		} 
		catch(ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch( SQLException e) {
			System.out.println("SQLException happened within init(), details follows ...");
			System.out.println(e.toString());
			return;
		}    
	}

    /**
     * destroy at aned of servlett life
     */
    public void 	destroy(){
    	System.out.println("\n\nok, servlet will be destroyed now ...\n");

    	try {
    		housenumberConn.close();
			System.out.println("after closed DB-Connection");
    	}
		catch( SQLException e) {
			System.out.println("SQLException happened within init(), details follows ...");
			System.out.println(e.toString());
			return;
		}    
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
		String fieldseparator = "\t";

		java.util.Date requestStarttime;
		java.util.Date requestEndtime;

		try {
			if(		(housenumberConn == null) || (housenumberConn.getMetaData() == null) || (housenumberConn.getMetaData().getURL() == null) 
				|| 	housenumberConn.getMetaData().getURL().equals("")) {
				Class.forName("org.postgresql.Driver");
				
				String url_hausnummern = configuration.db_application_url;
				housenumberConn = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);
			}
		} 
		catch(ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		catch( SQLException e) {
			System.out.println("SQLException happened within init(), details follows ...");
			System.out.println(e.toString());
			return;
		}    
		
		try {
			requestStarttime = new java.util.Date();

			System.out.println("\n\nBeginn getHousenumberlist/doPost v20180511 at " + requestStarttime.toString() + " ...");
			System.out.println("request komplett ===" + request.toString() + "===");

			String parameterCountry = URLDecoder.decode(request.getParameter("country"),"UTF-8");
			String parameterMunicipality = URLDecoder.decode(request.getParameter("municipality"),"UTF-8");
			String parameterOfficialkeysid = URLDecoder.decode(request.getParameter("officialkeysid"),"UTF-8");
			Integer parameterAdminlevel = Integer.parseInt(URLDecoder.decode(request.getParameter("adminlevel"),"UTF-8"));
			String parameterJobname = URLDecoder.decode(request.getParameter("jobname"),"UTF-8");
			long parameterJobId  = Long.parseLong(request.getParameter("job_id"));
			String parameterSubid = URLDecoder.decode(request.getParameter("subid"),"UTF-8");
			String parameterServerobjectid = URLDecoder.decode(request.getParameter("serverobjectid"),"UTF-8");

			System.out.println("=== input parameters for db without decoding ===");
			System.out.println(" country                 ===" + parameterCountry + "===");
			System.out.println(" municipality            ===" + parameterMunicipality + "===");
			System.out.println(" officialkeysid          ===" + parameterOfficialkeysid + "===");
			System.out.println(" adminlevel              ===" + parameterAdminlevel + "===");
			System.out.println(" jobname                 ===" + parameterJobname + "===");
			System.out.println(" job_id                  ===" + parameterJobId + "===");
			System.out.println(" subid      	         ===" + parameterSubid + "===");
			System.out.println(" parameterServerobjectid ===" + parameterServerobjectid + "===");

			System.out.println(" request encoding ===" + request.getCharacterEncoding());
			

			String selectMunicipalitySql = "SELECT land, stadt, muni.id as stadt_id, osm_hierarchy, officialkeys_id, " +
				"sourcelist_url, sourcelist_copyrighttext, sourcelist_useagetext, " +
				"sourcelist_contentdate, sourcelist_filedate, " +
				"subareajobs.admin_level, subareajobs.osm_id, " +
				"officialgeocoordinates, " +
				"jobs.id as municipality_jobid, jobname AS municipality_jobname " + 		//optional available columns (if gebiete and job creation run already) with job_id and name of municipality itself
				"FROM stadt AS muni " +
				"JOIN land as country " +
				"  ON muni.land_id = country.id " +
				"LEFT JOIN " +
				"  (SELECT  subarea.id AS id, name, admin_level, osm_id, subarea.stadt_id FROM " +
				"   jobs JOIN gebiete AS subarea ON jobs.gebiete_id = subarea.id " +
				"   JOIN stadt AS muni ON subarea.stadt_id = muni.id " +
				"   JOIN land AS country ON muni.land_id = country.id " +
				"     WHERE land = ? AND " +
				"     stadt = ? ";
			if(! parameterOfficialkeysid.equals(""))
				selectMunicipalitySql += "      AND officialkeys_id = ? ";
			selectMunicipalitySql += "      ORDER BY admin_level::int LIMIT 1) AS subareajobs " +		// just get most top admin_level row of gebiete
				"  ON subareajobs.stadt_id = muni.id " +
				"LEFT JOIN jobs " +
				"  ON jobs.gebiete_id = subareajobs.id " +
				"WHERE " +
				"country.land = ? " + 
				"AND muni.stadt = ? ";
			if(! parameterOfficialkeysid.equals(""))
				selectMunicipalitySql += "AND officialkeys_id = ? ";
			selectMunicipalitySql += ";";

			PreparedStatement selectMunicipalityStmt = housenumberConn.prepareStatement(selectMunicipalitySql);

			int preparedmuniindex = 1;
			String munipreparedParameters = "";
			selectMunicipalityStmt.setString(preparedmuniindex++, parameterCountry);
			selectMunicipalityStmt.setString(preparedmuniindex++, parameterMunicipality);
			if(! parameterOfficialkeysid.equals("")) {
				selectMunicipalityStmt.setString(preparedmuniindex++, parameterOfficialkeysid);
			}
			selectMunicipalityStmt.setString(preparedmuniindex++, parameterCountry);
			selectMunicipalityStmt.setString(preparedmuniindex++, parameterMunicipality);
			if(! parameterOfficialkeysid.equals("")) {
				selectMunicipalityStmt.setString(preparedmuniindex++, parameterOfficialkeysid);
			}
			System.out.println("municipality query: " + selectMunicipalityStmt.toString() + "===");


			ResultSet selectMunicipalityRS = selectMunicipalityStmt.executeQuery();

			int countMunicipalities = 0;
			boolean officialgeocoordinates = false;
			String country = "";
			String municipality = "";
			Long municipalityId = 0L;
			Long municipalityJobId = 0L;
			String municipalityJobname = "";
			String officialkeysid = "";
			String osmhierarchy = "";
			String sourcelisturl = "";
			String sourcelistcopyrighttext = "";
			String sourcelistuseagetext = "";
			String sourcelistcontentdate = "";
			String sourcelistfiledate = "";
			int subareaLevel = 0;
			Long subareaOsmid = 0L;
			while(selectMunicipalityRS.next()) {
				countMunicipalities++;
				country = selectMunicipalityRS.getString("land");
				municipality = selectMunicipalityRS.getString("stadt");
				municipalityId = selectMunicipalityRS.getLong("stadt_id");
				officialkeysid = selectMunicipalityRS.getString("officialkeys_id");
				osmhierarchy = selectMunicipalityRS.getString("osm_hierarchy");
				sourcelisturl = selectMunicipalityRS.getString("sourcelist_url");
				sourcelistcopyrighttext = selectMunicipalityRS.getString("sourcelist_copyrighttext");
				sourcelistuseagetext = selectMunicipalityRS.getString("sourcelist_useagetext");
				sourcelistcontentdate = selectMunicipalityRS.getString("sourcelist_contentdate");
				sourcelistfiledate = selectMunicipalityRS.getString("sourcelist_filedate");
				subareaLevel = selectMunicipalityRS.getInt("admin_level");
				subareaOsmid = selectMunicipalityRS.getLong("osm_id");
				if(		(selectMunicipalityRS.getString("officialgeocoordinates") != null)
					&& 	(selectMunicipalityRS.getString("officialgeocoordinates").equals("y"))) {
					officialgeocoordinates = true;
				}
				municipalityJobId = selectMunicipalityRS.getLong("municipality_jobid");
				if(selectMunicipalityRS.getString("municipality_jobname") != null)
					municipalityJobname = selectMunicipalityRS.getString("municipality_jobname");
			}
			System.out.println("municipalityJobId ===" + municipalityJobId + "===");
			System.out.println("municipalityJobname ===" + municipalityJobname + "===");
			selectMunicipalityStmt.close();
			requestEndtime = new java.util.Date();
			System.out.println("time for query for municipality in sec: " + (requestEndtime.getTime() - requestStarttime.getTime())/1000);
	
				// if found municipality don't have a job-id, it can't be processed below - stop here
			if(municipalityJobId == 0) {
				String errormessage = "Error: No jobs found to requested municipality '" + municipality
					+ "' in country '" + country + "', so housenumber list can't be delivered";
				System.out.println(errormessage);
				errormessage = " (cont.) subarea osm_id: " + subareaOsmid + ", admin_level: " + subareaLevel;
				System.out.println(errormessage);
				
					// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
				response.setContentType("text/plain;charset=UTF-8");
				response.setHeader("Content-Encoding", "UTF-8");
				response.setStatus(HttpServletResponse.SC_NOT_FOUND);
	
				PrintWriter writer = response.getWriter();
				writer.println(errormessage);
				writer.close();

					// change jobqueue entry to disable it for further processing
				if(parameterServerobjectid.indexOf("jobqueue:") == 0) {
					String serverobjectid_parts[] = parameterServerobjectid.split(":");
					if(serverobjectid_parts.length == 2) {
						String updateJobqueueSql = "UPDATE jobqueue set state = 'fail-nojob'";
						updateJobqueueSql += " WHERE";
						updateJobqueueSql += " id = ? AND";
						updateJobqueueSql += " state = 'open';";
						updateJobqueueSql += ";";
						PreparedStatement updateJobqueueStmt = housenumberConn.prepareStatement(updateJobqueueSql);
						updateJobqueueStmt.setLong(1, Long.parseLong(serverobjectid_parts[1]));
						System.out.println("change jobqueue Entry to fail with statement ===" +
							updateJobqueueStmt + "===");
						updateJobqueueStmt.executeUpdate();
						updateJobqueueStmt.close();
					} else {
						System.out.println("Error in getHousenumberlist: unknown structure in Serverobjectid, id complete ===" + parameterServerobjectid + "===, will be ignored");
					}
				} else {
					System.out.println("Warning in getHousenumberlist: unknown Serverobjectid Prefix, id complete ===" + parameterServerobjectid + "===, will be ignored");
				}

				System.out.println("End getHousenumberlist/doPost at " + requestEndtime.toString() 
					+ ",   Duration was " + (requestEndtime.getTime() - requestStarttime.getTime())+ " ms !");
				return;
			}
			
				// if more than one municipalities found, stop with a warning
			if(countMunicipalities > 1) {
				String errormessage = "Error: Number of municipalities, that fit to requested municipality '" + municipality
					+ "' in country '" + country + "' were more than one, so housenumber list can't be delivered";
				System.out.println(errormessage);
				
					// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
				response.setContentType("text/plain;charset=UTF-8");
				response.setHeader("Content-Encoding", "UTF-8");
				response.setStatus(HttpServletResponse.SC_NOT_FOUND);
	
				PrintWriter writer = response.getWriter();
				writer.println(errormessage);
				writer.close();

					// change jobqueue entry to disable it for further processing
				if(parameterServerobjectid.indexOf("jobqueue:") == 0) {
					String serverobjectid_parts[] = parameterServerobjectid.split(":");
					if(serverobjectid_parts.length == 2) {
						String updateJobqueueSql = "UPDATE jobqueue set state = 'fail-notuniquemunicipality'";
						updateJobqueueSql += " WHERE";
						updateJobqueueSql += " id = ? AND";
						updateJobqueueSql += " state = 'open';";
						updateJobqueueSql += ";";
						PreparedStatement updateJobqueueStmt = housenumberConn.prepareStatement(updateJobqueueSql);
						updateJobqueueStmt.setLong(1, Long.parseLong(serverobjectid_parts[1]));
						System.out.println("change jobqueue Entry to fail with statement ===" +
								updateJobqueueStmt + "===");
						updateJobqueueStmt.executeUpdate();
						updateJobqueueStmt.close();
					} else {
						System.out.println("Error in getHousenumberlist: unknown structure in Serverobjectid, id complete ===" + parameterServerobjectid + "===, will be ignored");
					}
				} else {
					System.out.println("Warning in getHousenumberlist: unknown Serverobjectid Prefix, id complete ===" + parameterServerobjectid + "===, will be ignored");
				}

				
				
				System.out.println("Ende getHousenumberlist/doPost at " + requestEndtime.toString() 
					+ ",   Duration was " + (requestEndtime.getTime() - requestStarttime.getTime())+ " ms !");
				return;
			}
			
			
			String selectPolygonSql = "SELECT g.polygon AS polygon900913, " +
				"ST_Transform(g.polygon,4326) AS polygon4326, " +
				"ST_GeometryType(g.polygon) AS polygontype, " +
				"j.id AS job_id, g.id AS subarea_dbid " +
				"FROM jobs AS j JOIN gebiete AS g ON j.gebiete_id = g.id " +
				"WHERE " +
				"j.id = ?;";
			PreparedStatement selectPolygonStmt = housenumberConn.prepareStatement(selectPolygonSql);
			selectPolygonStmt.setLong(1, municipalityJobId);
			System.out.println("polygon query: " + selectPolygonStmt.toString() + "===");

			requestStarttime = new java.util.Date();

			ResultSet selectPolygonRS = selectPolygonStmt.executeQuery();

			String polygon900913 = "";
			String polygon4326 = "";
			String polygontype = "";
			long subareaDbid = 0;
			long jobid = 0;
			while(selectPolygonRS.next()) {
				polygon900913 = selectPolygonRS.getString("polygon900913");
				polygon4326 = selectPolygonRS.getString("polygon4326");
				polygontype = selectPolygonRS.getString("polygontype");
				if(polygontype.indexOf("ST_") == 0)
					polygontype = "::" + polygontype.substring(3);	// take type of geometry starting at pos 3 after Prefix "ST_"
				jobid = selectPolygonRS.getLong("job_id");
				subareaDbid = selectPolygonRS.getLong("subarea_dbid");
			}
			selectPolygonStmt.close();
			requestEndtime = new java.util.Date();
			System.out.println("time for query for polygon in sec: " + (requestEndtime.getTime() - requestStarttime.getTime())/1000);


			String sqlqueryofficialhousenumbers = "";
			PreparedStatement queryofficialhousenumbersStmt;

	// is not in production, because Tomcat heap size exception and algorithm not finished completely
//officialgeocoordinates = false;

			requestStarttime = new java.util.Date();
				// if subadmin query for an official housenumber list, where 
				// - the subadmin area is not given,
				// - BUT geocoordinates are available for the official housenumbers (table stadt, column officialgeocoordinates, value = "y" new at 2015-02-09
			if(officialgeocoordinates) {
				sqlqueryofficialhousenumbers = "SELECT strasse, postcode, sh.hausnummer AS hausnummer, " +
				"sh.hausnummer_sortierbar AS hausnummer_sortierbar, " +
				"strasse, sh.sub_id AS sub_id, ST_X(point) as lon, ST_Y(point) as lat, pointsource, hausnummer_bemerkung " +
				"FROM stadt_hausnummern AS sh JOIN strasse AS str ON sh.strasse_id = str.id " +
				"JOIN stadt AS s ON sh.stadt_id = s.id " +
				"JOIN gebiete AS g ON g.stadt_id = s.id " +
				"JOIN jobs AS j ON j.gebiete_id = g.id " +
				"WHERE " +
				"j.id = ? AND ";
				if(! parameterSubid.equals("-1"))
					sqlqueryofficialhousenumbers += "(sh.sub_id = ? OR sh.sub_id = '-1') AND ";
				sqlqueryofficialhousenumbers += "ST_Within(point,?::geometry) ";
				sqlqueryofficialhousenumbers += "ORDER BY correctorder(strasse), hausnummer_sortierbar;";

				queryofficialhousenumbersStmt = housenumberConn.prepareStatement(sqlqueryofficialhousenumbers);
				int preparedindex = 1;
				queryofficialhousenumbersStmt.setLong(preparedindex++, jobid);
				if(! parameterSubid.equals("-1")) {
					queryofficialhousenumbersStmt.setString(preparedindex++, parameterSubid);
				}
				queryofficialhousenumbersStmt.setString(preparedindex++, polygon4326);
				System.out.println("official housenumber list query, case officialgeocoord. " + queryofficialhousenumbersStmt.toString() + "===");
				
			} else if(parameterAdminlevel <= 8) {
				sqlqueryofficialhousenumbers = "SELECT strasse, postcode, sh.hausnummer AS hausnummer, " +
					"sh.hausnummer_sortierbar AS hausnummer_sortierbar, " +
					"strasse, sh.sub_id, ST_X(point) as lon, ST_Y(point) as lat, pointsource, hausnummer_bemerkung " +
					"FROM stadt_hausnummern AS sh JOIN strasse AS str ON sh.strasse_id = str.id " +
					"JOIN stadt AS s ON sh.stadt_id = s.id " +
					"JOIN gebiete AS g ON g.stadt_id = s.id " +
					"JOIN jobs AS j ON j.gebiete_id = g.id " +
					"WHERE " +
					"j.id = ? ";
				if(! parameterSubid.equals("-1"))
					sqlqueryofficialhousenumbers += "AND ((sh.sub_id = ?) OR (sh.sub_id = '-1')) ";
				sqlqueryofficialhousenumbers += " ORDER BY correctorder(strasse), hausnummer_sortierbar;";

				queryofficialhousenumbersStmt = housenumberConn.prepareStatement(sqlqueryofficialhousenumbers);
				int preparedindex = 1;
				queryofficialhousenumbersStmt.setLong(preparedindex++, jobid);
				if(! parameterSubid.equals("-1")) {
					queryofficialhousenumbersStmt.setString(preparedindex++, parameterSubid);
				}
				System.out.println("official housenumber list query, case adminlevel <=8 " + queryofficialhousenumbersStmt.toString() + "===");

			} else {
					// if subadmin query for an official housenumber list, where 
					// - the subadmin area is not given,
					// - and no geocoordinates are available for the official housenumbers
					// then the official list must be filtered with the table jobs_strassen, where all osm streets (places are missing there up to now)
					// are found within the subadmin area osm polygon
					// this is the select statement for the dynamically selection of the housenumber list
				sqlqueryofficialhousenumbers = "SELECT strasse, postcode, sh.hausnummer AS hausnummer, " +
					"sh.hausnummer_sortierbar AS hausnummer_sortierbar, " +
					"strasse, sub_id, ST_X(point) as lon, ST_Y(point) as lat, pointsource, hausnummer_bemerkung " +
					"FROM stadt_hausnummern AS sh " +
					"JOIN strasse AS str ON sh.strasse_id = str.id " +
					"LEFT JOIN " +										// get all streets, which are completely outside subarea
					"  (SELECT DISTINCT ON (strasse_id) strasse_id " +
					"   FROM jobs_strassen WHERE stadt_id = ? AND " +
					"     job_id = ? AND " +
					"     NOT ( " +		//complete outside subarea
					"       ST_Within(linestring, ?::geometry) OR " +
					"       ST_Crosses(linestring, ?::geometry) " +
					"      ) " +
					"  ) AS outsidestreets " +
					"  ON sh.strasse_id = outsidestreets.strasse_id " +
					"LEFT JOIN " +										// get all OSM housenumbers, which are completely outside subarea
					"  (SELECT DISTINCT ON (strasse_id, hausnummer_sortierbar) strasse_id, hausnummer_sortierbar " +
					"   FROM auswertung_hausnummern " +
					"   WHERE " +
					"     stadt_id = ? AND " +
					"     job_id = ? AND " +
					"     NOT ST_Within(point, ?::geometry) " +		// only nodes, so no need for ST_Crosses()
					"  ) AS outsidehousenumbers " +
					"  ON sh.strasse_id = outsidehousenumbers.strasse_id AND " +
					"     sh.hausnummer_sortierbar = outsidehousenumbers.hausnummer_sortierbar " +
					"WHERE " +
					"sh.stadt_id = ? AND " +
					"outsidestreets.strasse_id is null AND " +		// in left join above, use only singles from stadt_hausnummern
					"outsidehousenumbers.strasse_id is null";	// in left join above, use only single housenumberrs, which are inside subarea
				if(! parameterSubid.equals("-1"))
					sqlqueryofficialhousenumbers += " AND (sub_id = ? OR sub_id = '-1')";
				sqlqueryofficialhousenumbers += " ORDER BY correctorder(strasse), hausnummer_sortierbar;";

				queryofficialhousenumbersStmt = housenumberConn.prepareStatement(sqlqueryofficialhousenumbers);
				int preparedindex = 1;
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityId);
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityJobId);
				queryofficialhousenumbersStmt.setString(preparedindex++, polygon900913);
				queryofficialhousenumbersStmt.setString(preparedindex++, polygon900913);
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityId);
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityJobId);
				queryofficialhousenumbersStmt.setString(preparedindex++, polygon4326);
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityId);
				if(! parameterSubid.equals("-1")) {
					queryofficialhousenumbersStmt.setString(preparedindex++, parameterSubid);
				}

				System.out.println("official housenumber list query, else case: " + sqlqueryofficialhousenumbers + "===");
				System.out.println("SQL-Parameters");
				System.out.println("                            municipalityId  (1): " + municipalityId);
				System.out.println("                          municipalityJobId (2): " + municipalityJobId);
				System.out.println("polygon in srid 900913 - table gebiete dbid (3): " + subareaDbid);
				System.out.println("polygon in srid 900913 - table gebiete dbid (4): " + subareaDbid);
				System.out.println("                             municipalityId (5): " + municipalityId);
				System.out.println("                          municipalityJobId (6): " + municipalityJobId);
				System.out.println("polygon in srid   4326 - table gebiete dbid (7): " + subareaDbid);
				System.out.println("                             municipalityId (8): " + municipalityId);
				if(! parameterSubid.equals("-1")) {
					System.out.println("parameterSubid: " + parameterSubid);
				}
			}

			ResultSet rsqueryofficialhousenumbers = queryofficialhousenumbersStmt.executeQuery();

			StringBuffer dataoutput = new StringBuffer();

			int rownumber = 0;
			while(rsqueryofficialhousenumbers.next()) {
				rownumber++;
				String actoutputline = "";
				if(rownumber == 1) {
					actoutputline = "#" 
						+ "Subadminarea" + fieldseparator
						+ "Street" + fieldseparator
						+ "Housenumber" + fieldseparator
						+ "LonLat" + fieldseparator
						+ "LonLatSource" + fieldseparator
						+ "HousenumberComment" + fieldseparator
						+ "Postcode";
					dataoutput.append(actoutputline + "\n");

					dataoutput.append("#Para Country=" + country + "\n");
					dataoutput.append("#Para Municipality=" + municipality + "\n");
					dataoutput.append("#Para Officialkeysid=" + officialkeysid + "\n");
					dataoutput.append("#Para OSMHierarchy=" + osmhierarchy + "\n");
					dataoutput.append("#Para sourcelisturl=" + sourcelisturl + "\n");
					dataoutput.append("#Para legal Copyright Text=" + sourcelistcopyrighttext + "\n");
					dataoutput.append("#Para legal Useage Note=" + sourcelistuseagetext + "\n");
					dataoutput.append("#Para sourcelistcontentdate=" + sourcelistcontentdate + "\n");
					dataoutput.append("#Para sourcelistfiledate=" + sourcelistfiledate + "\n");
				}

				String lonlat = "";
				String pointsource = "";
				String housenumbercomment = "";
				String postcode = "";
				if(rsqueryofficialhousenumbers.getString("lon") != null)
					lonlat = rsqueryofficialhousenumbers.getString("lon") + " " + rsqueryofficialhousenumbers.getString("lat");
				if(rsqueryofficialhousenumbers.getString("pointsource") != null)
					pointsource = rsqueryofficialhousenumbers.getString("pointsource");
				if(rsqueryofficialhousenumbers.getString("hausnummer_bemerkung") != null)
					housenumbercomment = rsqueryofficialhousenumbers.getString("hausnummer_bemerkung"); 
				if(rsqueryofficialhousenumbers.getString("postcode") != null)
					postcode = rsqueryofficialhousenumbers.getString("postcode"); 
				
				actoutputline = rsqueryofficialhousenumbers.getString("sub_id") + "\t"
					+ rsqueryofficialhousenumbers.getString("strasse") + "\t"
					+ rsqueryofficialhousenumbers.getString("hausnummer") + "\t"
					+ lonlat + "\t"
					+ pointsource + "\t"
					+ housenumbercomment + "\t"
					+ postcode;
				dataoutput.append(actoutputline + "\n");
			}
			requestEndtime = new java.util.Date();
			System.out.println("time for query for housenumerlist in sec: " + (requestEndtime.getTime() - requestStarttime.getTime())/1000);
			//System.out.println("result content ===" + dataoutput.toString() + "===");
			System.out.println("Housenumber list file length: " + dataoutput.toString().length());
			System.out.println("Number of housenumbers: " + rownumber);

			queryofficialhousenumbersStmt.close();

			
			
				// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
			response.setContentType("text/plain;charset=UTF-8");
			response.setHeader("Content-Encoding", "UTF-8");

			PrintWriter writer = response.getWriter();
			writer.println(dataoutput.toString());
			writer.close();

				// if job is from jobqueue table, then update state of job
			if(!parameterServerobjectid.equals("")) {
				System.out.println("after response stream closed now work on available serverobjectid ===" + parameterServerobjectid + "===");
				if(parameterServerobjectid.indexOf("jobqueue:") == 0) {
					String serverobjectid_parts[] = parameterServerobjectid.split(":");
					if(serverobjectid_parts.length == 2) {
						String updateJobqueueSql = "UPDATE jobqueue set state = 'started'";
						updateJobqueueSql += " WHERE";
						updateJobqueueSql += " id = ? AND";
						updateJobqueueSql += " state = 'open';";
						updateJobqueueSql += ";";
						PreparedStatement updateJobqueueStmt = housenumberConn.prepareStatement(updateJobqueueSql);
						updateJobqueueStmt.setLong(1, Long.parseLong(serverobjectid_parts[1]));
						updateJobqueueStmt.executeUpdate();
						updateJobqueueStmt.close();
					} else {
						System.out.println("Error in getHousenumberlist: unknown structure in Serverobjectid, id complete ===" + parameterServerobjectid + "===, will be ignored");
					}
				} else {
					System.out.println("Warning in getHousenumberlist: unknown Serverobjectid Prefix, id complete ===" + parameterServerobjectid + "===, will be ignored");
				}
				System.out.println("after response end of work on available serverobjectid ===" + parameterServerobjectid + "===");
			}
		
			System.out.println("\nEnde getHousenumberlist/doPost at " + new Date());
		
		} // end of try to connect to DB and operate with DB
		catch( PSQLException psqle) {
			System.out.println("Error: PSQLException happened, Details follows ...");
			System.out.println(psqle.toString());
			response.setContentType("text/plain; charset=utf-8");
			response.sendError(432, "OSM-Data Error");
			PrintWriter writer = response.getWriter();
			writer.println("PSQLException happened, details follows ...");
			writer.println(psqle.toString());
			writer.close();
			return;
		}
		catch( SQLException e) {
			System.out.println("Error: SQLException happened, Details follows ...");
			System.out.println(e.toString());
			response.setContentType("text/plain; charset=utf-8");
			response.sendError(431, "Database Error");
			PrintWriter writer = response.getWriter();
			//response.setHeader("Database Error",  "430");
			writer.println("SQLException happened, details follows ...");
			writer.println(e.toString());
			writer.close();
			return;
		}
		catch( OutOfMemoryError memoryerror) {
			System.out.println("OutOfMemoryError exception happened, details follow ...");
			memoryerror.printStackTrace();
			response.setContentType("text/plain; charset=utf-8");
			response.sendError(430, "Server Application Problem");
			PrintWriter writer = response.getWriter();
			writer.println("SQLException happened, details follows ...");
			writer.println(memoryerror.toString());
			writer.close();
			return;
		}
	}
}

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

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
	static Connection con_hausnummern;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public getHousenumberlist() {
        super();
        // TODO Auto-generated constructor stub
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
			System.out.println("request komplett ===" + request.toString() + "===");
			System.out.println("ok, in doPost angekommen ...");

			String path = request.getServletContext().getRealPath("/WEB-INF");
			configuration = new Applicationconfiguration(path);
			System.out.println("ok, nach ende setzen configuraiton");

			String parameterCountry = URLDecoder.decode(request.getParameter("country"),"UTF-8");
			String parameterMunicipality = URLDecoder.decode(request.getParameter("municipality"),"UTF-8");
			String parameterOfficialkeysid = URLDecoder.decode(request.getParameter("officialkeysid"),"UTF-8");
			Integer parameterAdminlevel = Integer.parseInt(URLDecoder.decode(request.getParameter("adminlevel"),"UTF-8"));
			String parameterJobname = URLDecoder.decode(request.getParameter("jobname"),"UTF-8");
			String parameterSubid = URLDecoder.decode(request.getParameter("subid"),"UTF-8");
			 
			
			System.out.println("=== input parameters for db without decoding ===");
			System.out.println(" country        ===" + parameterCountry + "===");
			System.out.println(" municipality   ===" + parameterMunicipality + "===");
			System.out.println(" officialkeysid ===" + parameterOfficialkeysid + "===");
			System.out.println(" adminlevel     ===" + parameterAdminlevel + "===");
			System.out.println(" jobname        ===" + parameterJobname + "===");
			System.out.println(" subid          ===" + parameterSubid + "===");

			System.out.println(" request encoding ===" + request.getCharacterEncoding());
			
			  
			  
			Class.forName("org.postgresql.Driver");
	
			String url_hausnummern = configuration.db_application_url;
			con_hausnummern = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);

			String selectMunicipalitySql = "SELECT land, stadt, s.id as stadt_id, osm_hierarchy, officialkeys_id,";
			selectMunicipalitySql += " sourcelist_url, sourcelist_copyrighttext, sourcelist_useagetext,";
			selectMunicipalitySql += " sourcelist_contentdate, sourcelist_filedate,";
			selectMunicipalitySql += " officialgeocoordinates,";
			selectMunicipalitySql += " j.id as municipality_jobid, jobname AS municipality_jobname";		//optional available columns (if gebiete and job creation run already) with job_id and name of municipality itself
			selectMunicipalitySql += " FROM stadt AS s";
			selectMunicipalitySql += " JOIN land as l";
			selectMunicipalitySql += "   ON s.land_id = l.id";
			selectMunicipalitySql += " LEFT JOIN";
			selectMunicipalitySql += "   (SELECT  gebiete.id AS id, name, stadt_id FROM gebiete";
			selectMunicipalitySql += "    JOIN stadt on gebiete.stadt_id = stadt.id";
			selectMunicipalitySql += "    JOIN land on stadt.land_id = land.id";
			selectMunicipalitySql += "      WHERE land = ? AND";
			selectMunicipalitySql += "      stadt = ?";
			if(! parameterOfficialkeysid.equals(""))
				selectMunicipalitySql += "      AND officialkeys_id = ?";
			selectMunicipalitySql += "      ORDER BY admin_level::int LIMIT 1) AS gebietejobs";		// just get most top admin_level row of gebiete
			selectMunicipalitySql += "   ON gebietejobs.stadt_id = s.id";
			selectMunicipalitySql += " LEFT JOIN jobs AS j";
			selectMunicipalitySql += "   ON j.gebiete_id = gebietejobs.id";
			selectMunicipalitySql += " WHERE";
			selectMunicipalitySql += " land = ? AND";
			selectMunicipalitySql += " stadt = ?";
			if(! parameterOfficialkeysid.equals(""))
				selectMunicipalitySql += " AND officialkeys_id = ?";
			selectMunicipalitySql += ";";
			PreparedStatement selectMunicipalityStmt = con_hausnummern.prepareStatement(selectMunicipalitySql);

			
			
			int preparedmuniindex = 1;
			String munipreparedParameters = "";
			selectMunicipalityStmt.setString(preparedmuniindex++, parameterCountry);
			munipreparedParameters += ", country='" + parameterCountry + "'";
			selectMunicipalityStmt.setString(preparedmuniindex++, parameterMunicipality);
			munipreparedParameters += ", municipality='" + parameterMunicipality + "'";
			if(! parameterOfficialkeysid.equals("")) {
				selectMunicipalityStmt.setString(preparedmuniindex++, parameterOfficialkeysid);
				munipreparedParameters += ", officialkeysid='" + parameterOfficialkeysid + "'";
			}
			selectMunicipalityStmt.setString(preparedmuniindex++, parameterCountry);
			munipreparedParameters += ", country='" + parameterCountry + "'";
			selectMunicipalityStmt.setString(preparedmuniindex++, parameterMunicipality);
			munipreparedParameters += ", municipality='" + parameterMunicipality + "'";
			if(! parameterOfficialkeysid.equals("")) {
				selectMunicipalityStmt.setString(preparedmuniindex++, parameterOfficialkeysid);
				munipreparedParameters += ", officialkeysid='" + parameterOfficialkeysid + "'";
			}
			System.out.println("municipality query: Parameters " + munipreparedParameters + "     ===" + selectMunicipalitySql + "===");

			requestStarttime = new java.util.Date();

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
				if(		(selectMunicipalityRS.getString("officialgeocoordinates") != null)
					&& 	(selectMunicipalityRS.getString("officialgeocoordinates").equals("y"))) {
					officialgeocoordinates = true;
				}
				municipalityJobId = selectMunicipalityRS.getLong("municipality_jobid");
				if(selectMunicipalityRS.getString("municipality_jobname") != null)
					municipalityJobname = selectMunicipalityRS.getString("municipality_jobname");
			}
System.out.println("municipalityJobId: " + municipalityJobId + ", municipalityJobname ===" + municipalityJobname + "===");
			selectMunicipalityStmt.close();
			requestEndtime = new java.util.Date();
			System.out.println("time for query for municipality in sec: " + (requestEndtime.getTime() - requestStarttime.getTime())/1000);

			if(countMunicipalities > 1) {
				String errormessage = "Error: Number of municipalities, that fit to requested municipality '" + municipality
					+ "' in country '" + country + "' were more than one, so housenumber list can't be delivered";
				System.out.println(errormessage);
				con_hausnummern.close();
				
					// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
				response.setContentType("text/plain;charset=UTF-8");
				response.setHeader("Content-Encoding", "UTF-8");
				response.setStatus(HttpServletResponse.SC_NOT_FOUND);
	
				PrintWriter writer = response.getWriter();
				writer.println(errormessage);
				writer.close();
				return;
			}
			
			
			String selectPolygonSql = "SELECT g.polygon as polygon900913,";
			selectPolygonSql += " ST_Transform(g.polygon,4326) AS polygon4326,";
			selectPolygonSql += " ST_GeometryType(g.polygon) AS polygontype,";
			selectPolygonSql += " j.id as job_id";
			selectPolygonSql += " FROM jobs AS j, gebiete AS g, stadt AS s, land AS l";
			selectPolygonSql += " WHERE";
			selectPolygonSql += " j.gebiete_id = g.id AND";
			selectPolygonSql += " j.stadt_id = s.id AND";
			selectPolygonSql += " s.land_id = l.id AND";
			selectPolygonSql += " land = ? AND";
			selectPolygonSql += " stadt = ? AND";
			if(! parameterOfficialkeysid.equals(""))
				selectPolygonSql += " officialkeys_id = ? AND";
			selectPolygonSql += " admin_level = ? AND";
			selectPolygonSql += " jobname = ?";
			selectPolygonSql += ";";
			PreparedStatement selectPolygonStmt = con_hausnummern.prepareStatement(selectPolygonSql);


			int preparedpolyindex = 1;
			String polypreparedParameters = "";
			selectPolygonStmt.setString(preparedpolyindex++, parameterCountry);
			polypreparedParameters += ", country='" + parameterCountry + "'";
			selectPolygonStmt.setString(preparedpolyindex++, parameterMunicipality);
			polypreparedParameters += ", municipality='" + parameterMunicipality + "'";
			if(! parameterOfficialkeysid.equals("")) {
				selectPolygonStmt.setString(preparedpolyindex++, parameterOfficialkeysid);
				polypreparedParameters += ", officialkeysid='" + parameterOfficialkeysid + "'";
			}
			selectPolygonStmt.setInt(preparedpolyindex++, parameterAdminlevel);
			polypreparedParameters += ", adminlevel='" + parameterAdminlevel + "'";
			selectPolygonStmt.setString(preparedpolyindex++, parameterJobname);
			polypreparedParameters += ", jobname='" + parameterJobname + "'";
			System.out.println("polygon query: Parameters " + polypreparedParameters + "     ===" + selectPolygonSql + "===");

			requestStarttime = new java.util.Date();

			ResultSet selectPolygonRS = selectPolygonStmt.executeQuery();

			String polygon900913 = "";
			String polygon4326 = "";
			String polygontype = "";
			Long jobid = 0L;
			while(selectPolygonRS.next()) {
				polygon900913 = selectPolygonRS.getString("polygon900913");
				polygon4326 = selectPolygonRS.getString("polygon4326");
				polygontype = selectPolygonRS.getString("polygontype");
				if(polygontype.indexOf("ST_") == 0)
					polygontype = "::" + polygontype.substring(3);	// take type of geometry starting at pos 3 after Prefix "ST_"
				jobid = selectPolygonRS.getLong("job_id");
			}
			selectPolygonStmt.close();
			requestEndtime = new java.util.Date();
			System.out.println("time for query for polygon in sec: " + (requestEndtime.getTime() - requestStarttime.getTime())/1000);


			String sqlqueryofficialhousenumbers = "";
			PreparedStatement queryofficialhousenumbersStmt;

	// is not in production, because Tomcat heap size exception and algorithm not finished completely
officialgeocoordinates = false;	

			int preparedindex = 1;
			String listpreparedParameters = "";
			requestStarttime = new java.util.Date();
				// if subadmin query for an official housenumber list, where 
				// - the subadmin area is not given,
				// - BUT geocoordinates are available for the official housenumbers (table stadt, column officialgeocoordinates, value = "y" new at 2015-02-09
			if(officialgeocoordinates) {
				sqlqueryofficialhousenumbers = "SELECT strasse, sh.hausnummer AS hausnummer,";
				sqlqueryofficialhousenumbers += " sh.hausnummer_sortierbar AS hausnummer_sortierbar,";
				sqlqueryofficialhousenumbers += " strasse, sh.sub_id AS sub_id, ST_X(point) as lon, ST_Y(point) as lat, pointsource, hausnummer_bemerkung";
				sqlqueryofficialhousenumbers += " FROM stadt_hausnummern AS sh,";
				sqlqueryofficialhousenumbers += " strasse AS str,";
				sqlqueryofficialhousenumbers += " jobs AS j,";
				sqlqueryofficialhousenumbers += " gebiete AS g,";
				sqlqueryofficialhousenumbers += " stadt AS s,";
				sqlqueryofficialhousenumbers += " land as l";
				sqlqueryofficialhousenumbers += " WHERE";
				sqlqueryofficialhousenumbers += " land = ? AND";
				sqlqueryofficialhousenumbers += " stadt = ? AND";
				if(! officialkeysid.equals(""))
					sqlqueryofficialhousenumbers += " officialkeys_id = ? AND";
				sqlqueryofficialhousenumbers += " j.id = ? AND";
				if(! parameterSubid.equals("-1"))
					sqlqueryofficialhousenumbers += " (sh.sub_id = ? OR sh.sub_id = '-1') AND";
				sqlqueryofficialhousenumbers += " sh.land_id = l.id AND";
				sqlqueryofficialhousenumbers += " sh.stadt_id = s.id AND";
				sqlqueryofficialhousenumbers += " sh.strasse_id = str.id AND";
				sqlqueryofficialhousenumbers += " j.gebiete_id = g.id AND";
				sqlqueryofficialhousenumbers += " g.stadt_id = s.id AND";
				sqlqueryofficialhousenumbers += " ST_Within(point,?::geometry)";
				if(parameterAdminlevel > 8) {
					sqlqueryofficialhousenumbers += " AND job_id = ?";
				}
				sqlqueryofficialhousenumbers += " ORDER BY correctorder(strasse), hausnummer_sortierbar;";

				queryofficialhousenumbersStmt = con_hausnummern.prepareStatement(sqlqueryofficialhousenumbers);
				preparedindex = 1;
				queryofficialhousenumbersStmt.setString(preparedindex++, country);
				listpreparedParameters += ", country='" + country + "'";
				queryofficialhousenumbersStmt.setString(preparedindex++, municipality);
				listpreparedParameters += ", municipality='" + municipality + "'";
				if(! officialkeysid.equals("")) {
					queryofficialhousenumbersStmt.setString(preparedindex++, officialkeysid);
					listpreparedParameters += ", officialkeysid='" + officialkeysid + "'";
				}
				if(! parameterSubid.equals("-1")) {
					queryofficialhousenumbersStmt.setString(preparedindex++, parameterSubid);
//check, if sub_id or job_id must be used here
					listpreparedParameters += ", subid='" + parameterSubid + "'";
				}
				queryofficialhousenumbersStmt.setString(preparedindex++, polygon4326);
				listpreparedParameters += ", polygon4326='...'";
				if(parameterAdminlevel > 8) {
					queryofficialhousenumbersStmt.setLong(preparedindex++, jobid);
					listpreparedParameters += ", jobid='" + jobid + "'";
				}
			} else if(parameterAdminlevel <= 8) {
				sqlqueryofficialhousenumbers = "SELECT strasse, sh.hausnummer AS hausnummer,";
				sqlqueryofficialhousenumbers += " sh.hausnummer_sortierbar AS hausnummer_sortierbar,";
				sqlqueryofficialhousenumbers += " strasse, sub_id, ST_X(point) as lon, ST_Y(point) as lat, pointsource, hausnummer_bemerkung";
				sqlqueryofficialhousenumbers += " FROM stadt_hausnummern AS sh,";
				sqlqueryofficialhousenumbers += " strasse AS str,";
				sqlqueryofficialhousenumbers += " stadt AS s,";
				sqlqueryofficialhousenumbers +=	 " land as l";
				sqlqueryofficialhousenumbers += " WHERE";
				sqlqueryofficialhousenumbers += " land = ? AND";
				sqlqueryofficialhousenumbers += " stadt = ? AND";
				if(! officialkeysid.equals(""))
					sqlqueryofficialhousenumbers += " officialkeys_id = ? AND";
				if(! parameterSubid.equals("-1"))
					sqlqueryofficialhousenumbers += " (sub_id = ? OR sub_id = '-1') AND";
				sqlqueryofficialhousenumbers += " sh.land_id = l.id AND";
				sqlqueryofficialhousenumbers += " sh.stadt_id = s.id AND";
				sqlqueryofficialhousenumbers += " sh.strasse_id = str.id";
				sqlqueryofficialhousenumbers += " ORDER BY correctorder(strasse), hausnummer_sortierbar;";

				queryofficialhousenumbersStmt = con_hausnummern.prepareStatement(sqlqueryofficialhousenumbers);
				preparedindex = 1;
				queryofficialhousenumbersStmt.setString(preparedindex++, country);
				listpreparedParameters += ", country='" + country + "'";
				queryofficialhousenumbersStmt.setString(preparedindex++, municipality);
				listpreparedParameters += ", municipality='" + municipality + "'";
				if(! officialkeysid.equals("")) {
					queryofficialhousenumbersStmt.setString(preparedindex++, officialkeysid);
					listpreparedParameters += ", officialkeysid='" + officialkeysid + "'";
				}
				if(! parameterSubid.equals("-1")) {
					queryofficialhousenumbersStmt.setString(preparedindex++, parameterSubid);
					listpreparedParameters += ", subid='" + parameterSubid + "'";
				}
			} else {
					// if subadmin query for an official housenumber list, where 
					// - the subadmin area is not given,
					// - and no geocoordinates are available for the official housenumbers
					// then the official list must be filtered with the table jobs_strassen, where all osm streets (places are missing there up to now)
					// are found within the subadmin area osm polygon
					// this is the select statement for the dynamically selection of the housenumber list
				sqlqueryofficialhousenumbers = "SELECT strasse, sh.hausnummer AS hausnummer,"
					+ " sh.hausnummer_sortierbar AS hausnummer_sortierbar,"
					+ " strasse, sub_id, ST_X(point) as lon, ST_Y(point) as lat, pointsource, hausnummer_bemerkung"
					+ " FROM stadt_hausnummern AS sh"
					+ " JOIN strasse AS str"
					+ "   ON sh.strasse_id = str.id"
					+ " LEFT JOIN"										// get all streets, which are completely outside subarea
					+ "   (SELECT DISTINCT ON (strasse_id) strasse_id"
					+ "    FROM jobs_strassen WHERE stadt_id = ? AND"
					+ "      job_id = ? AND"
					+ "      NOT ("		//complete outside subarea
					+ "        ST_Within(linestring, ?::geometry) OR"
					+ "        ST_Crosses(linestring, ?::geometry)"
					+ "      )"
					+ "   ) AS outsidestreets"
					+ "   ON sh.strasse_id = outsidestreets.strasse_id"
					+ " LEFT JOIN"										// get all OSM housenumbers, which are completely outside subarea
					+ "   (SELECT DISTINCT ON (strasse_id, hausnummer_sortierbar) strasse_id, hausnummer_sortierbar"
					+ "    FROM auswertung_hausnummern"
					+ "    WHERE"
					+ "      stadt_id = ? AND"
					+ "      job_id = ? AND"
					+ "      NOT ST_Within(point, ?::geometry)"		// only nodes, so no need for ST_Crosses()
					+ "   ) AS outsidehousenumbers"
					+ "   ON sh.strasse_id = outsidehousenumbers.strasse_id AND"
					+ "      sh.hausnummer_sortierbar = outsidehousenumbers.hausnummer_sortierbar"
					+ " WHERE"
					+ " sh.stadt_id = ? AND"
					+ " outsidestreets.strasse_id is null AND"		// in left join above, use only singles from stadt_hausnummern
					+ " outsidehousenumbers.strasse_id is null";	// in left join above, use only single housenumberrs, which are inside subarea
				if(! parameterSubid.equals("-1"))
					sqlqueryofficialhousenumbers += " AND (sub_id = ? OR sub_id = '-1')";
				sqlqueryofficialhousenumbers += " ORDER BY correctorder(strasse), hausnummer_sortierbar;";

				queryofficialhousenumbersStmt = con_hausnummern.prepareStatement(sqlqueryofficialhousenumbers);
				preparedindex = 1;
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityId);
				listpreparedParameters += ", municipalityId=" + municipalityId + " (for " + municipality + ")";
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityJobId);
				listpreparedParameters += ", municipalityJobId=" + municipalityJobId + " (for " + municipalityJobname + ")";
				queryofficialhousenumbersStmt.setString(preparedindex++, polygon900913);
				listpreparedParameters += ", polygon900913='...'";
				queryofficialhousenumbersStmt.setString(preparedindex++, polygon900913);
				listpreparedParameters += ", polygon900913='...'";
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityId);
				listpreparedParameters += ", municipalityId=" + municipalityId + " (for " + municipality + ")";
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityJobId);
				listpreparedParameters += ", municipalityJobId=" + municipalityJobId + " (for " + municipalityJobname + ")";
				queryofficialhousenumbersStmt.setString(preparedindex++, polygon4326);
				listpreparedParameters += ", polygon4326='...'";
				queryofficialhousenumbersStmt.setLong(preparedindex++, municipalityId);
				listpreparedParameters += ", municipalityId=" + municipalityId + " (for " + municipality + ")";
				if(! parameterSubid.equals("-1")) {
					queryofficialhousenumbersStmt.setString(preparedindex++, parameterSubid);
					listpreparedParameters += ", subid='" + parameterSubid + "'";
				}
			}

			System.out.println("official housenumber list query: Parameters " + listpreparedParameters + "     ===" + sqlqueryofficialhousenumbers + "===");

			ResultSet rsqueryofficialhousenumbers = queryofficialhousenumbersStmt.executeQuery();

			StringBuffer dataoutput = new StringBuffer();

			int rownumber = 0;
			while(rsqueryofficialhousenumbers.next()) {
				rownumber++;
				String actoutputline = "";
System.out.println("row #" + rownumber);
				if(rownumber == 1) {
					actoutputline = "#" 
						+ "Subadminarea" + fieldseparator
						+ "Street" + fieldseparator
						+ "Housenumber" + fieldseparator
						+ "LonLat" + fieldseparator
						+ "LonLatSource" + fieldseparator
						+ "HousenumberComment";
					dataoutput.append(actoutputline + "\n");

					dataoutput.append("#Para Country=" + country + "\n");
					dataoutput.append("#Para Municipality=" + municipality + "\n");
					dataoutput.append("#Para Officialkeysid=" + officialkeysid + "\n");
					dataoutput.append("#Para OSMHierarchy=" + osmhierarchy + "\n");
					dataoutput.append("#Para sourcelisturl=" + sourcelisturl + "\n");
					dataoutput.append("#Para sourcelistcopyrighttext=" + sourcelistcopyrighttext + "\n");
					dataoutput.append("#Para sourcelistuseagetext=" + sourcelistuseagetext + "\n");
					dataoutput.append("#Para sourcelistcontentdate=" + sourcelistcontentdate + "\n");
					dataoutput.append("#Para sourcelistfiledate=" + sourcelistfiledate + "\n");
				}

				String lonlat = "";
				String pointsource = "";
				String housenumbercomment = "";
				if(rsqueryofficialhousenumbers.getString("lon") != null)
					lonlat = rsqueryofficialhousenumbers.getString("lon") + " " + rsqueryofficialhousenumbers.getString("lat");
				if(rsqueryofficialhousenumbers.getString("pointsource") != null)
					pointsource = rsqueryofficialhousenumbers.getString("pointsource");
				if(rsqueryofficialhousenumbers.getString("hausnummer_bemerkung") != null)
					housenumbercomment = rsqueryofficialhousenumbers.getString("hausnummer_bemerkung"); 
				
				actoutputline = rsqueryofficialhousenumbers.getString("sub_id") + "\t"
					+ rsqueryofficialhousenumbers.getString("strasse") + "\t"
					+ rsqueryofficialhousenumbers.getString("hausnummer") + "\t"
					+ lonlat + "\t"
					+ pointsource + "\t"
					+ housenumbercomment;
				dataoutput.append(actoutputline + "\n");
			}
			requestEndtime = new java.util.Date();
			System.out.println("time for query for housenumerlist in sec: " + (requestEndtime.getTime() - requestStarttime.getTime())/1000);
			//System.out.println("result content ===" + dataoutput.toString() + "===");
			System.out.println("Housenumber list file length: " + dataoutput.toString().length());
			System.out.println("Number of housenumbers: " + rownumber);

			queryofficialhousenumbersStmt.close();
			con_hausnummern.close();

			
			
				// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
			response.setContentType("text/plain;charset=UTF-8");
			response.setHeader("Content-Encoding", "UTF-8");

			PrintWriter writer = response.getWriter();
			writer.println(dataoutput.toString());
			writer.close();
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
			PrintWriter writer = response.getWriter();
			response.setContentType("text/plain; charset=utf-8");
			response.setHeader("Accept",  "400");
			writer.println("SQLException happened, details follows ...");
			writer.println(e.toString());
			writer.close();
			return;
		}
		catch( OutOfMemoryError memoryerror) {
			System.out.println("OutOfMemoryError exception happened, details follow ...");
			memoryerror.printStackTrace();
			PrintWriter writer = response.getWriter();
			response.setContentType("text/plain; charset=utf-8");
			response.setHeader("Accept",  "400");
			writer.println("SQLException happened, details follows ...");
			writer.println(memoryerror.toString());
			writer.close();
			return;
		}
	}
}

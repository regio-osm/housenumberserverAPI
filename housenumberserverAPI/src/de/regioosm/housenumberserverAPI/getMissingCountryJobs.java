package de.regioosm.housenumberserverAPI;

import java.io.IOException;
import java.io.PrintWriter;

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
@WebServlet("/getMissingCountryJobs")
	//	parameters for external class, which interpret the client request:
	//	location: to which temporary directory, a file can be created and stored.
	//	maxFileSize: up to which size, the upload file can be. CAUTION: as of 2015-01-11, the upload file is not compressed
@MultipartConfig(location = "/tmp", maxFileSize = 20971520) // 20MB.
public class getMissingCountryJobs extends HttpServlet {
	private static final long serialVersionUID = 1L;
		// load content of configuration file, which contains filesystem entries and database connection details
	static Applicationconfiguration configuration;
	static Connection con_hausnummern;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public getMissingCountryJobs() {
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

		try {
			System.out.println("\nBeginn getMissingCountryJobs/doPost ... " + new Date());
			System.out.println("request komplett ===" + request.toString() + "===");
			System.out.println("ok, in doPost angekommen ...");

			String path = request.getServletContext().getRealPath("/WEB-INF");
			System.out.println("path for/WEB-INF ===" + path + "===");
			configuration = new Applicationconfiguration(path);
			
			MultipartMap map = new MultipartMap(request, this);

			System.out.println("nach multipartmap in doPost ...");

			String country = map.getParameter("country");

			// Now do your thing with the obtained input.
			System.out.println("=== original input parameters ===");
			System.out.println(" country      ===" + country + "===");

			country = country.replace("*", "%");

			System.out.println("=== changed input parameters for db ===");
			System.out.println(" country      ===" + country + "===");

			Class.forName("org.postgresql.Driver");
	
			String url_hausnummern = configuration.db_application_url;
			con_hausnummern = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);

// query for all MAIN municipality (admin_level=7) missing jobs
// select land, stadt, officialkeys_id, jobname, sub_id, osm_id * -1 from jobs, gebiete, stadt, land where jobs.gebiete_id = gebiete.id and gebiete.stadt_id = stadt.id and stadt.land_id = land.id and land = 'Poland' and admin_level = 7 and jobs.id not in (select distinct(j.id) from evaluations as e, jobs as j, gebiete as g, stadt as s where e.job_id = j.id and j.gebiete_id = g.id and g.stadt_id = s.id and s.land_id = 7);
			
			String select_sql = "SELECT land.id AS countryid, land, countrycode,"
				+ " stadt.id AS municipalityid, stadt, officialkeys_id, admin_level,"
				+ " jobs.id AS jobid, jobname, osm_id, sub_id"
				+ " FROM jobs, gebiete, stadt, land"
				+ " WHERE stadt.land_id = land.id AND"
				+ " land = ?"
				+ " AND jobs.stadt_id = stadt.id"
				+ " AND jobs.gebiete_id = gebiete.id"
				+ " AND jobs.id NOT IN"
				+ " (SELECT e.job_id FROM"
				+ " evaluations AS e, jobs AS j, gebiete AS g, stadt AS s, land as l"
				+ " WHERE e.job_id = j.id AND"
				+ " j.gebiete_id = g.id AND"
				+ " g.stadt_id = s.id AND"
				+ " s.land_id = l.id AND"
				+ " land = ?)"
				+ " ORDER BY stadt, officialkeys_id, jobname;";

			System.out.println("SQL-Query to find requested jobs for client ===" + select_sql + "===");
			PreparedStatement selectqueryStmt = con_hausnummern.prepareStatement(select_sql);
			selectqueryStmt.setString(1, country);
			selectqueryStmt.setString(2, country);
			ResultSet existingmunicipalityRS = selectqueryStmt.executeQuery();

			StringBuffer dataoutput = new StringBuffer();
			String actoutputline = "";

			actoutputline = "#" + "Country\tCountrycode\tMunicipality\tMunicipality-Id\tAdmin-Level"
				+ "\tJobname\tSubarea-Id\tOSM-Relation-Id\n";
			dataoutput.append(actoutputline);

			while(existingmunicipalityRS.next()) {
				Long osm_id = existingmunicipalityRS.getLong("osm_id");
				if(osm_id < 0)
					osm_id = Math.abs(osm_id);
				
				actoutputline = existingmunicipalityRS.getString("land") + "\t"
					+ existingmunicipalityRS.getString("countrycode") + "\t"
					+ existingmunicipalityRS.getString("stadt") + "\t"
					+ existingmunicipalityRS.getString("officialkeys_id") + "\t"
					+ existingmunicipalityRS.getInt("admin_level") + "\t"
					+ existingmunicipalityRS.getString("jobname") + "\t"
					+ existingmunicipalityRS.getString("sub_id") + "\t"
					+ osm_id + "\n";
				dataoutput.append(actoutputline);
			}
			System.out.println("result content ===" + dataoutput.toString() + "===");

			selectqueryStmt.close();
			con_hausnummern.close();

				// output Character Encoding MUST BE SET PREVIOUSLY to response.getWriter to work !!!
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
		} catch (ServletException se) {
			System.out.println("ServletException happened, details follows ...");
			System.out.println("  .. details ===" + se.toString() + "===");
			se.printStackTrace();
			PrintWriter writer = response.getWriter();
			response.setContentType("text/plain; charset=utf-8");
			response.setHeader("Accept",  "400");
			writer.println("ServletException happened, details follows ...");
			writer.println(se.toString());
			writer.close();
			return;
		}
	}
}

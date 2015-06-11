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
@WebServlet("/getqueuejobs")
	//	parameters for external class, which interpret the client request:
	//	location: to which temporary directory, a file can be created and stored.
	//	maxFileSize: up to which size, the upload file can be. CAUTION: as of 2015-01-11, the upload file is not compressed
@MultipartConfig(location = "/tmp", maxFileSize = 20971520) // 20MB.	
public class getQueueJobs extends HttpServlet {
	private static final long serialVersionUID = 1L;
		// load content of configuration file, which contains filesystem entries and database connection details
	static Applicationconfiguration configuration;
	static Connection con_hausnummern;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public getQueueJobs() {
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
		writer.println("	<h1>Hello in Method doGet of class getQueueJob.java!</h1>");
		writer.println("<body>");
		writer.println("</html>");
			
		writer.close();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {


		try {
			System.out.println("Beginn getQueueJobs/doPost ...");
			System.out.println("request komplett ===" + request.toString() + "===");

			String path = request.getServletContext().getRealPath("/WEB-INF");
			configuration = new Applicationconfiguration(path);
			
			MultipartMap map = new MultipartMap(request, this);

			System.out.println("nach multipartmap in doPost ...");

			String requestreason = map.getParameter("requestreason");
			Integer maxjobcount = Integer.parseInt(map.getParameter("maxjobcount"));


			System.out.println("=== input parameters ===");
			System.out.println(" requestreason ===" + requestreason + "===");
			System.out.println(" maxjobcount   ===" + maxjobcount + "===");

			Class.forName("org.postgresql.Driver");
	
			String url_hausnummern = configuration.db_application_url;
			con_hausnummern = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);

			String select_queuejobssql = "SELECT id, countrycode, municipality, jobname, requestreason,"
				+ " requesttime, scheduletime, priority, state"
				+ " FROM jobqueue WHERE"
				+ " state = 'open' AND"
				+ " (";
			if(		requestreason.equals("regular") 
				||	requestreason.equals("")) {
				select_queuejobssql += " (scheduletime < now())";
			}
			if(	requestreason.equals("")) {
				select_queuejobssql += " OR";
			}
			if(		requestreason.equals("instant") 
					||	requestreason.equals("")) {
				select_queuejobssql += " (requestreason = 'instant')";
			}
			select_queuejobssql += ")"
				+ " ORDER BY priority DESC, scheduletime, requesttime"
				+ " LIMIT ?;";

			System.out.println("SQL-Query to get jobs from queue ===" + select_queuejobssql + "===");
			PreparedStatement selectqueuejobsqueryStmt = con_hausnummern.prepareStatement(select_queuejobssql);
			selectqueuejobsqueryStmt.setInt(1, maxjobcount);
			ResultSet queuejobsqueryRS = selectqueuejobsqueryStmt.executeQuery();

			PreparedStatement selectqueryStmt = null;
			ResultSet existingmunicipalityRS = null;
			StringBuffer dataoutput = new StringBuffer();

			Integer rowcount = 0;
			while(queuejobsqueryRS.next()) {
				
				String select_sql = "SELECT land.id AS countryid, land,"
					+ " stadt.id AS municipalityid, stadt, officialkeys_id, admin_level,"
					+ " jobs.id AS jobid, jobname, osm_id, sub_id"
					+ " FROM land, stadt, gebiete, jobs WHERE"
					+ " stadt ilike ?"
					+ " AND countrycode = ?"
					+ " AND jobname ilike ?"
					+ " AND stadt.land_id = land.id"
					+ " AND gebiete.stadt_id = stadt.id"
					+ " AND jobs.gebiete_id = gebiete.id"
					+ " ORDER BY land, stadt, admin_level, jobname;";		// sort admin_level asc is important, so that main evaluation of a municipality will be make first

				selectqueryStmt = con_hausnummern.prepareStatement(select_sql);
				String existingpreparedParameters = "";
				String actvalue = "%";
				if(queuejobsqueryRS.getString("municipality") != null) {
					actvalue = queuejobsqueryRS.getString("municipality");
					actvalue = actvalue.replace("*","%");
				}
				selectqueryStmt.setString(1, actvalue);
				existingpreparedParameters += ", municipality='" + actvalue + "'";
				actvalue = "%";
				if(queuejobsqueryRS.getString("countrycode") != null) {
					actvalue = queuejobsqueryRS.getString("countrycode");
					actvalue = actvalue.replace("*","%");
				}
				selectqueryStmt.setString(2, actvalue);
				existingpreparedParameters += ", countrycode='" + actvalue + "'";
				actvalue = "%";
				if(queuejobsqueryRS.getString("jobname") != null) {
					actvalue = queuejobsqueryRS.getString("jobname");
					actvalue = actvalue.replace("*","%");
				}
				selectqueryStmt.setString(4, actvalue);
				existingpreparedParameters += ", jobname='" + actvalue + "'";
				System.out.println("existing muni query: Parameters " + existingpreparedParameters + "     ===" + select_sql + "===");
				existingmunicipalityRS = selectqueryStmt.executeQuery();
			

				while(existingmunicipalityRS.next()) {
					String actoutputline = "";
					Long osm_id = existingmunicipalityRS.getLong("osm_id");
					if(osm_id < 0)
						osm_id = Math.abs(osm_id);

					actoutputline = existingmunicipalityRS.getString("land") + "\t"
						+ existingmunicipalityRS.getString("stadt") + "\t"
						+ existingmunicipalityRS.getString("officialkeys_id") + "\t"
						+ existingmunicipalityRS.getInt("admin_level") + "\t"
						+ existingmunicipalityRS.getString("jobname") + "\t"
						+ existingmunicipalityRS.getString("sub_id") + "\t"
						+ osm_id + "\t"
						+ "jobqueue:"+ queuejobsqueryRS.getInt("id") + "\n";
					dataoutput.append(actoutputline);
				}
				rowcount++;
				if(rowcount > maxjobcount) {
					System.out.println("Info: stop adding result jobs, limit reached");
					break;
				}
			}
			System.out.println("result content ===" + dataoutput.toString() + "===");


				// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
			response.setContentType("text/plain;charset=UTF-8");
			response.setHeader("Content-Encoding", "UTF-8");
			response.setStatus(HttpServletResponse.SC_OK);
			
			PrintWriter writer = response.getWriter();
			writer.println(dataoutput.toString());
			writer.close();

			System.out.println("after close of response stream and before close of DB-connection ...");
			
			
			System.out.println("before selectqueryStmt.close");
			selectqueryStmt.close();
			System.out.println("after selectqueryStmt.close");
			selectqueuejobsqueryStmt.close();
			System.out.println("after selectqueuejobsqueryStmt.close");
			con_hausnummern.close();

			System.out.println("after closed DB-Connection");
			
			System.out.println("after closed DB-Connection and at end of method");
			System.out.println("Ende getQueueJobs/doPost!");
			return;
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
			System.out.println("Ende getQueueJobs/doPost!");
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
			System.out.println("Ende getQueueJobs/doPost!");
			return;
		} catch (Error e) {
			System.out.println("general Exception happened, details follows ...");
			System.out.println("  .. details ===" + e.toString() + "===");
			e.printStackTrace();
			PrintWriter writer = response.getWriter();
			response.setContentType("text/plain; charset=utf-8");
			response.setHeader("Accept",  "400");
			writer.println("ServletException happened, details follows ...");
			writer.println(e.toString());
			writer.close();
			System.out.println("Ende getQueueJobs/doPost!");
			return;
		}
	}
}

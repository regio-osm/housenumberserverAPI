package de.regioosm.housenumberserverAPI;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
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
     * initialization on servlett startup
     */
    public void 	init(ServletConfig config) {
    	System.out.println("\n\nok, servlet v20170107 " + config.getServletName() + " will be initialized now ...\n");

		String path = config.getServletContext().getRealPath("/WEB-INF");
		configuration = new Applicationconfiguration(path);

		try {
		Class.forName("org.postgresql.Driver");
		
		String url_hausnummern = configuration.db_application_url;
		con_hausnummern = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);
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
    		con_hausnummern.close();
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
		writer.println("	<h1>Hello in Method doGet of class getQueueJob.java!</h1>");
		writer.println("<body>");
		writer.println("</html>");
			
		writer.close();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String select_queuejobssql = "";
		String select_sql = "";
		String existingpreparedParameters = "";

		PreparedStatement selectqueuejobsqueryStmt = null;
		ResultSet queuejobsqueryRS = null;
		
		try {
			System.out.println("\nBeginn getQueueJobs/doPost v20170107 ... " + new Date());
			System.out.println("request komplett ===" + request.toString() + "===");

			MultipartMap map = new MultipartMap(request, this);

			System.out.println("nach multipartmap in doPost ...");

			String requestreason = map.getParameter("requestreason");
			String country = map.getParameter("country");
			Integer maxjobcount = Integer.parseInt(map.getParameter("maxjobcount"));

			if((country == null) || country.equals(""))
				country = "%";

			System.out.println("=== input parameters ===");
			System.out.println(" requestreason ===" + requestreason + "===");
			System.out.println(" country ===" + country + "===");
			System.out.println(" maxjobcount   ===" + maxjobcount + "===");


			select_queuejobssql = "SELECT jq.id AS id, jq.countrycode AS countrycode, municipality, jobname, requestreason,"
				+ " requesttime, scheduletime, priority, state"
				+ " FROM jobqueue AS jq, land AS l WHERE"
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
				+ " AND jq.countrycode = l.countrycode"
				+ " AND l.land like ?"
				+ " ORDER BY scheduletime, requesttime, priority DESC"
				+ " LIMIT ?;";

			selectqueuejobsqueryStmt = con_hausnummern.prepareStatement(select_queuejobssql);
			
			selectqueuejobsqueryStmt.setString(1, country);
			selectqueuejobsqueryStmt.setInt(2, maxjobcount);
			existingpreparedParameters = "";
			existingpreparedParameters += "country='" + country + "'";
			existingpreparedParameters += ", maxjobcount=" + maxjobcount;

			queuejobsqueryRS = selectqueuejobsqueryStmt.executeQuery();
			System.out.println("SQL-Query to get jobs from queue: Parameters ===" + existingpreparedParameters  + "=== query ===" + select_queuejobssql + "===");

			StringBuffer dataoutput = new StringBuffer();
			String actoutputline = "";
			
			actoutputline = "#" + "Country\tCountrycode\tMunicipality\tMunicipality-Id\tAdmin-Level"
				+ "\tJobname\tSubarea-Id\tOSM-Relation-Id\tJobqueue-Id\tJobid\n";
			dataoutput.append(actoutputline);
			
			Integer rowcount = 0;
			while(queuejobsqueryRS.next()) {
				
				select_sql = "SELECT land.id AS countryid, land, countrycode,"
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

				PreparedStatement JobQueryStmt = con_hausnummern.prepareStatement(select_sql);
				existingpreparedParameters = "";
				String actvalue = "%";
				if(queuejobsqueryRS.getString("municipality") != null) {
					actvalue = queuejobsqueryRS.getString("municipality");
					actvalue = actvalue.replace("*","%");
				}
				JobQueryStmt.setString(1, actvalue);
				existingpreparedParameters += "municipality='" + actvalue + "'";
				actvalue = "%";
				if(queuejobsqueryRS.getString("countrycode") != null) {
					actvalue = queuejobsqueryRS.getString("countrycode");
					actvalue = actvalue.replace("*","%");
				}
				JobQueryStmt.setString(2, actvalue);
				existingpreparedParameters += ", countrycode='" + actvalue + "'";
				actvalue = "%";
				if(queuejobsqueryRS.getString("jobname") != null) {
					actvalue = queuejobsqueryRS.getString("jobname");
					actvalue = actvalue.replace("*","%");
				}
				JobQueryStmt.setString(3, actvalue);
				existingpreparedParameters += ", jobname='" + actvalue + "'";

				System.out.println("existing muni query: Parameters " + existingpreparedParameters + "     \n===" + select_sql + "===");
				ResultSet jobQueryRS = JobQueryStmt.executeQuery();
				select_sql = "";
				existingpreparedParameters = "";
				while(jobQueryRS.next()) {
					Long osm_id = jobQueryRS.getLong("osm_id");
					if(osm_id < 0)
						osm_id = Math.abs(osm_id);

					actoutputline = jobQueryRS.getString("land") + "\t"
						+ jobQueryRS.getString("countrycode") + "\t"
						+ jobQueryRS.getString("stadt") + "\t"
						+ jobQueryRS.getString("officialkeys_id") + "\t"
						+ jobQueryRS.getInt("admin_level") + "\t"
						+ jobQueryRS.getString("jobname") + "\t"
						+ jobQueryRS.getString("sub_id") + "\t"
						+ osm_id + "\t"
						+ "jobqueue:"+ queuejobsqueryRS.getInt("id") + "\t"
						+ jobQueryRS.getString("jobid")
						+ "\n";
					dataoutput.append(actoutputline);
					System.out.println("rowcount now " + (rowcount + 1) + ":   dataoutput.append now with ===" + actoutputline + "===    length: " + dataoutput.length());
					rowcount++;
				}
				if(rowcount > maxjobcount) {
					System.out.println("Info: stop adding result jobs, limit reached, dataoutput.length: " + dataoutput.length());
					break;
				}
				jobQueryRS.close();
				System.out.println("before JobQueryStmt.close");
				JobQueryStmt.close();
				System.out.println("after JobQueryStmt.close, dataoutput.length: " + dataoutput.length());
			}
			System.out.println("after job-loop result content ===" + dataoutput.toString() + "=== length: " + dataoutput.length());
			queuejobsqueryRS.close();
			selectqueuejobsqueryStmt.close();


				// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
			response.setContentType("text/plain;charset=UTF-8");
			response.setHeader("Content-Encoding", "UTF-8");
			response.setStatus(HttpServletResponse.SC_OK);
			
			PrintWriter writer = response.getWriter();
			writer.println(dataoutput.toString());
			writer.close();

			System.out.println("after close of response stream");

			selectqueuejobsqueryStmt.close();
			System.out.println("after selectqueuejobsqueryStmt.close");
			System.out.println("Ende getQueueJobs/doPost at " + new Date());
			return;
		} // end of try to connect to DB and operate with DB
		catch( SQLException e) {
			System.out.println("SQLException happened, details follows ...");
			System.out.println("sql query parameters ===" + existingpreparedParameters + "===");
			System.out.println("sql query was probably ===" + select_sql + "===");
			System.out.println("sql job-query was probably ===" + select_queuejobssql + "===");
			e.printStackTrace();
			try {
				if(queuejobsqueryRS != null)
					queuejobsqueryRS.close();
				if(selectqueuejobsqueryStmt != null)
					selectqueuejobsqueryStmt.close();
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

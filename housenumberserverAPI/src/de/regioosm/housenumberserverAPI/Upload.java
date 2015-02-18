package de.regioosm.housenumberserverAPI;

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
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

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
	static Applicationconfiguration configuration;
	static Connection con_hausnummern;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public Upload() {
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

		String filename = "";
		Integer linenumbers = 0;
		Long uploadlength = 0L;
		try {
			System.out.println("request kompletto ===" + request.toString() + "===");
			System.out.println("ok, in doPost angekommen ...");

			String path = request.getServletContext().getRealPath("/WEB-INF");
			configuration = new Applicationconfiguration(path);

			MultipartMap map = new MultipartMap(request, this);

			String country = map.getParameter("Country");
			String municipality = map.getParameter("Municipality");
			String officialkeysId = map.getParameter("Officialkeysid");
			String jobname = map.getParameter("Jobname");
			File file = map.getFile("result");
			System.out.println("temporary file ===" + file.getAbsoluteFile() + "===");


/*			GZIPInputStream gis=new GZIPInputStream(new FileInputStream(file.getAbsoluteFile()));
			byte[] buffer = new byte[10000000];
			gis.read(buffer);
			int numberBytes = gis.read();
			gis.close();
			System.out.println("numberBytes: " + numberBytes);
			System.out.println("decompressed content ===" + buffer.toString() + "===");
*/
			
			BufferedReader filereader = new BufferedReader(new InputStreamReader(new FileInputStream(file.getAbsoluteFile()),StandardCharsets.UTF_8));
		    String fileline = "";
		    StringBuffer filecontent = new StringBuffer();
		    linenumbers = 0;
		    while ((fileline = filereader.readLine()) != null) {
		    	linenumbers++;
		    	uploadlength += fileline.length();
		    	filecontent.append(fileline + "\n");
			}
			filereader.close();

			String content = filecontent.toString();

			// Now do your thing with the obtained input.
			System.out.println(" country        ===" + country + "===");
			System.out.println(" municipality   ===" + municipality + "===");
			System.out.println(" officialkeysId ===" + officialkeysId + "===");
			System.out.println(" jobname        ===" + jobname + "===");
			System.out.println(" content length ===" + content.length() + "===");
			System.out.println(" content file line no ===" + linenumbers + "===");


				// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
			response.setContentType("text/html; charset=utf-8");
			response.setHeader("Content-Encoding", "UTF-8");
			
			PrintWriter writer = response.getWriter();


			writer.println("<html>");
			writer.println("<head><meta charset=\"utf-8\"><title>doPost aktiv</title></head>");
			writer.println("<body>");
			writer.println("	<h1>Upload.java!</h1>");


			DateFormat time_formatter = new SimpleDateFormat("yyyyMMdd-HHmmssZ");
			String uploadtime = time_formatter.format(new Date());

			filename += configuration.upload_homedir + File.separator + "open";
			filename += "/" + uploadtime + ".result";
			System.out.println("uploaddatei ===" + filename + "===");

			File outputfile = new File(filename);
			outputfile.createNewFile();
			outputfile.setReadable(true, false);
			outputfile.setWritable(true, false);
			outputfile.setExecutable(false);

			PrintWriter uploadOutput = null;
			uploadOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filename),StandardCharsets.UTF_8)));
			uploadOutput.println(content);
			uploadOutput.close();

				// at 2015-01-20, inserted here to send the response to the client
				//			AFTER the result file has been imported into the DB,
				//			because otherwise, not all imports are successfully
//			System.out.println(" Sub-fkt insertResultIntoDB wird aufgerufen ...");
//			System.out.println(insertResultIntoDB(content, country, municipality, jobname));
//			System.out.println(" Sub-fkt insertResultIntoDB ist fertig");

			writer.println(" country        ===" + country + "===");
			writer.println(" municipality   ===" + municipality + "===");
			writer.println(" officialkeysId ===" + officialkeysId + "===");
			writer.println(" jobname        ===" + jobname + "===");
			writer.println(" content length ===" + content.length() + "===");

			writer.println("<body>");
			writer.println("</html>");
				
			writer.close();
		} catch (IOException ioerror) {
			System.out.println("ERORR: IOException happened, details follows ...");
			System.out.println(" .. couldn't open file to write, filename was ===" + filename + "===");
			System.out.println(" .. couldn't open file to write, filename was ===" + ioerror.toString() + "===");
			ioerror.printStackTrace();
		} catch (ServletException se) {
			System.out.println("ServletException happened, details follows ...");
			System.out.println("  .. details ===" + se.toString() + "===");
			se.printStackTrace();
		} catch (OutOfMemoryError memoryerror) {
			System.out.println("OutOfMemoryError happened, details follows ...");
			System.out.println("  .. details:  linenumbers: " + linenumbers + "; uploadlength: " + uploadlength);
			memoryerror.printStackTrace();
				// output Character Encoding MUST BE SET previously to response.getWriter to work !!!
			response.setContentType("text/html; charset=utf-8");
			response.setHeader("Content-Encoding", "UTF-8");
			response.setStatus(response.SC_INTERNAL_SERVER_ERROR);
			PrintWriter writer = response.getWriter();
			writer.println("<html>");
			writer.println("<head><meta charset=\"utf-8\"><title>doPost aktiv</title></head>");
			writer.println("<body>");
			writer.println("	<h1>Upload.java!</h1>");
			writer.println("<body>");
			writer.println("</html>");
			writer.close();
		}
	}
}

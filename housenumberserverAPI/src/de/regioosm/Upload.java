package de.regioosm;

import java.io.BufferedReader;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import javax.servlet.http.Part;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;

import de.diesei.hausnummern.Applicationconfiguration;
import net.balusc.http.multipart.MultipartMap;


/**
 * Servlet implementation class Upload
 */
@WebServlet("/Upload")
@MultipartConfig(location = "/tmp", maxFileSize = 20971520) // 20MB.	
public class Upload extends HttpServlet {
	private static final long serialVersionUID = 1L;

	static Applicationconfiguration configuration = new Applicationconfiguration();
	static Connection con_hausnummern;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public Upload() {
        super();
        // TODO Auto-generated constructor stub
    }


    
    private String setHausnummerNormalisiert(String hausnummer) {
		String hausnummersortierbar = "";
    	if(hausnummer != "") {
    		int numstellen = 0;
    		for(int posi=0;posi<hausnummer.length();posi++) {
    			int charwert = hausnummer.charAt(posi);
    			//System.out.println("aktuelle Textstelle ==="+in_hausnummer.charAt(posi)+"===  char-nr.: "+charwert);
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


    private String insertResultIntoDB(String content, String country, String municipality) {
		java.util.Date insertResultIntoDBouter = new java.util.Date();
		java.util.Date insertResultIntoDBinner = new java.util.Date();

		if(content.length() == 0) {
			System.out.println("leerer Content, Abbruch");
			return "leer Content, Abbruch";
		}

		
		try {
System.out.println("vor postgres-driver ...");
			Class.forName("org.postgresql.Driver");
	
			String url_hausnummern = configuration.db_application_url;
System.out.println("vor getconnection ...");
System.out.println("url_hausnummern===" + url_hausnummern + "===");
System.out.println("configuration.db_application_username ===" + configuration.db_application_username + "===");
System.out.println("configuration.db_application_password ===" + configuration.db_application_password + "===");
			con_hausnummern = DriverManager.getConnection(url_hausnummern, configuration.db_application_username, configuration.db_application_password);
			con_hausnummern.setAutoCommit(false);
	
			
			String select_sql = "SELECT land.id AS countryid, stadt.id AS municipalityid, jobs.id AS jobid"
				+ " FROM land, stadt, gebiete, jobs WHERE"
				+ " land = '" + country.replace("'", "''") + "'"
				+ " AND stadt = '" + municipality.replace("'", "''") + "'"
				+ " AND gebiete.stadt_id = stadt.id"
				+ " AND jobs.gebiete_id = gebiete.id"
				+ " AND stadt = jobname"
				+ " ORDER BY admin_level;";
System.out.println("first sql query to get country, muni and job ... ===" + select_sql + "===");

			int countryid = 0;
			int municipalityid = 0;
			int jobid = 0;
	
			Statement stmtOsmfehlendestrassen = con_hausnummern.createStatement();
System.out.println(" vor executequery ...");
			ResultSet rsOsmfehlendestrassen = stmtOsmfehlendestrassen.executeQuery(select_sql);
System.out.println(" nach executequery ...");
					//TODO check and code, what happens with more than one row
			if (rsOsmfehlendestrassen.next()) {
System.out.println("in next");
				countryid = rsOsmfehlendestrassen.getInt("countryid");
				municipalityid = rsOsmfehlendestrassen.getInt("municipalityid");
				jobid = rsOsmfehlendestrassen.getInt("jobid");
			} else {
				return "Fehler: die Stadt ist nicht in der Hausnummern-DB vorhanden, der Import kann nicht erfolgen";
			}
String output = "countryid ===" + countryid + "===   municipalityid ===" + municipalityid + "===   jobid ===" + jobid + "===";
System.out.println(output);

			System.out.println(" vor create statement");
			Statement createorupdateStmt = con_hausnummern.createStatement();


			String deleteauswertunghausnummern_sql = "DELETE FROM auswertung_hausnummern"
				+ " WHERE land_id = " + countryid + " AND stadt_id = " + municipalityid + " AND job_id = " + jobid + ";";
			System.out.println("deleteauswertunghausnummern statement ===" + deleteauswertunghausnummern_sql + "===");
			try {
				createorupdateStmt.executeUpdate( deleteauswertunghausnummern_sql );
			}
			catch( SQLException e) {
				System.out.println("ERROR: during insert in table osmhausnummern_hausnummern identische, insert code was ===" + deleteauswertunghausnummern_sql + "===");
				e.printStackTrace();
			}


			HashMap<String, Integer> street_idlist = new HashMap<String, Integer>();
			HashMap<Integer, String> headerfield = new HashMap<Integer, String>();
			String lines[] = content.split("\n");
System.out.println("Länge Content: " + content.length());
System.out.println("Inhalt Content (max. 1000 zeichen): " + content.substring(1,1000));
System.out.println("Anzahl Zeilen in Content: " + lines.length);
		
			for(int lineindex = 0; lineindex < lines.length; lineindex++) {
				String actline = lines[lineindex];
				if(actline == "")
					continue;
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
				if(actline.indexOf("#") == 0)
					continue;
				String columns[] = actline.split("\t");
				if(columns.length < 6) {
					System.out.println("FEHLER: Datensatz hat zuwenig Spalten, Zeile: " + lineindex + "  zeileninhalt ===" + actline + "===");
					continue;
				}
				HashMap<String, String> field = new HashMap<String, String>();
				for(int colindex = 0; colindex < columns.length; colindex++) {
					String actcolumn = columns[colindex];
					if(actcolumn == "")
						continue;
					field.put(headerfield.get(colindex), actcolumn);
					System.out.println("stored field[" + headerfield.get(colindex) + "] ===" + actcolumn + "===");
				}
//if(lineindex > 20) {
//	System.out.println("abbruch in Schleife nach 20 Durchgängen");
//	break;
//}
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
//INFO total time node.js server insert 22750 rows for Würzburg needs 13.6 minutes
//INFO total time java servlet server insert 22751 rows for Würzburg needs 11,7 minutes (without transaction)
//INFO total time java servlet server insert 22751 rows for Würzburg needs 7,9 minutes (withtransaction)

				if(! street_idlist.containsKey(actstreet)) {
					select_sql = "SELECT id FROM strasse where strasse = '" + actstreet.replace("'", "''") + "';";
					rsOsmfehlendestrassen = stmtOsmfehlendestrassen.executeQuery(select_sql);
					if (rsOsmfehlendestrassen.next()) {
						street_idlist.put(actstreet, rsOsmfehlendestrassen.getInt("id"));
					} else {
						String insert_sql = "INSERT INTO strasse (strasse) VALUES ('" + actstreet.replace("'", "''") + "');";
						System.out.println("insert_sql statement ===" + insert_sql + "===");
						String[] dbautogenkeys = { "id" };
						try {
							createorupdateStmt.executeUpdate( insert_sql, dbautogenkeys );
							ResultSet rs_getautogenkeys = createorupdateStmt.getGeneratedKeys();
						    if (rs_getautogenkeys.next()) {
								street_idlist.put(actstreet, rs_getautogenkeys.getInt("id"));
						    } 
						    rs_getautogenkeys.close();
						}
						catch( SQLException e) {
							System.out.println("ERROR: during insert in table evaluation_overview, insert code was ===" + insert_sql + "===");
							System.out.println(e.toString());
						}
					}
				}

				String insert_sql = "INSERT INTO auswertung_hausnummern"
					+ " (land_id, stadt_id, job_id, copyland, copystadt, copystrasse, strasse_id, hausnummer, hausnummer_sortierbar, treffertyp)"
					+ " VALUES("
					+ countryid + ","
					+ municipalityid + "," 
					+ jobid + ","
					+ "'" + country.replace("'", "''") + "',"
					+ "'" + municipality.replace("'", "''") + "'," 
					+ "'" + actstreet.replace("'", "''") + "',"
					+ street_idlist.get(actstreet) + ","
					+ "'" + acthousenumber + "',"
					+ "'" + acthousenumbersorted + "',"
					+ "'" + acthittype + "'"
					+ ");";
				System.out.println("insert_sql statement ===" + insert_sql + "===");

				try {
					java.util.Date time_beforeinsert = new java.util.Date();
					createorupdateStmt.executeUpdate( insert_sql );
					java.util.Date time_afterinsert = new java.util.Date();
					System.out.println("TIME for insert record osmhausnummern_hausnummern " + actstreet + " " + acthousenumber + " " + acthittype
						+ ", in ms. "+(time_afterinsert.getTime()-time_beforeinsert.getTime()));
				}
				catch( SQLException e) {
					System.out.println("ERROR: during insert in table osmhausnummern_hausnummern identische, insert code was ===" + insert_sql + "===");
					e.printStackTrace();
				}
			} // end of loop over all content lines
			con_hausnummern.commit();
			System.out.println("time for insertResultIntoDBinner in sek: " + (new Date().getTime() - insertResultIntoDBinner.getTime())/1000);

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

	
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void _____doPost_____worksforsmallcontent_____(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		String country = request.getParameter("country");
		String municipality = request.getParameter("municipality");
		String content = "";

		request.setCharacterEncoding(StandardCharsets.UTF_8.toString());

		PrintWriter writer = response.getWriter();

		Enumeration paramNames = request.getParameterNames();
		
		response.setContentType("text/html; charset=utf-8");

		writer.println("<html>");
		writer.println("<head><meta charset=\"utf-8\"><title>doPost aktiv</title></head>");
		writer.println("<body>");
		writer.println("	<h1>Hello World und Otto from a Servlet in Upload.java!</h1>");

		
		writer.println("<table width=\"100%\" border=\"1\" align=\"center\">\n" +
	        "<tr bgcolor=\"#949494\">\n" +
	        "<th>Param Name</th><th>Param Value(s)</th>\n"+
	        "</tr>\n");		
	      
		while(paramNames.hasMoreElements()) {
			String paramName = (String)paramNames.nextElement();
			writer.print("<tr><td>" + paramName + "</td>\n<td>");
			String[] paramValues =
			        request.getParameterValues(paramName);
			 // Read single valued data
			if (paramValues.length == 1) {
				String paramValue = paramValues[0];
				if (paramValue.length() == 0)
					writer.println("<i>No Value</i>");
				else
					writer.println(paramValue.length() + "</td></tr>\n");
			} else {
			     // Read multiple valued data
				writer.println("<ul>");
				for(int i=0; i < paramValues.length; i++) {
					writer.println("<li>" + paramValues[i].length());
				}
				writer.println("</ul></td></tr>\n");
			}
			
			if(paramName.indexOf("#") == 0) {
				content += paramName + "\n";
				content += paramValues[0] + "\n";
			}
		}
		writer.println("</table>\n");
		writer.println("<p>" + request.getCharacterEncoding() + "</p>\n");

	    Part filePart = request.getPart("file"); // Retrieves <input type="file" name="file">
	    System.out.println("im getpart file länge: " + filePart.getSize());
	    InputStream fileContent = filePart.getInputStream();
		content = fileContent.toString();

		String filename = "";
		try {
			DateFormat time_formatter = new SimpleDateFormat("yyyyMMdd-HHmmssZ");
			String uploadtime = time_formatter.format(new Date());

			filename += "/home/openstreetmap/NASI/OSMshare/programme/workspace/housenumberserverJavaServlet/uploaddata";
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
		} catch (IOException ioerror) {
			System.out.println("ERROR: couldn't open file to write, filename was ===" + filename + "===");
			ioerror.printStackTrace();
		}
		


		writer.println(" country ===" + country + "===");
		writer.println(" municipality ===" + municipality + "===");
		//writer.println(" content ===" + content + "===");

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
		
	
	}
	
	
}

package de.regioosm.housenumberserverAPI;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Properties;
import java.util.logging.Level;

import javax.servlet.http.HttpServlet;


public class Applicationconfiguration {

	public String servername = "";
	public String application_homedir = "";
	public String application_datadir = "";
	public String upload_homedir = "";
	public String db_structure = "osm2pgsql";
	public String db_application_url = "";
	public String db_application_username = "";
	public String db_application_password = "";
	public String db_osm2pgsql_url = "";
	public String db_osm2pgsql_username = "";
	public String db_osm2pgsql_password = "";
	public String db_osm2pgsqlwrite_username = "";
	public String db_osm2pgsqlwrite_password = "";
	public String osmosis_laststatefile = "";
	public String logging_filename = "";
	public Level logging_console_level = Level.FINEST;
	public Level logging_file_level = Level.FINEST;
	

	public Applicationconfiguration (String path) {
		boolean debugoutput = false;

		String configuration_filename = "";

		System.out.println("path to configuration file as method start ===" + path + "===");
		String userdir = System.getProperty("user.dir");
		System.out.println("current dir, is it good?   ===" + userdir);
		
		// get some configuration infos
		if(userdir.indexOf("tomcat") != -1) {
			configuration_filename =  userdir + File.separator + "webapps" + File.separator + "housenumberserverAPI.properties";
			System.out.println("found tomcat in userdir");
		} else if(path.length() > 2)
			configuration_filename =  path + File.separator + "housenumberserverAPI.properties";
		else
			configuration_filename =  userdir + File.separator + ".." + File.separator + "housenumberserverAPI.properties";

		if(debugoutput)
			System.out.println("configuration_filename ===" + configuration_filename+ "===");

		try {
			Reader reader = new FileReader( configuration_filename );
			Properties prop = new Properties();
			prop.load( reader );
			prop.list( System.out );
		

			if( prop.getProperty("servername") != null)
				this.servername = prop.getProperty("servername");
			if( prop.getProperty("db_structure") != null)
				this.db_structure = prop.getProperty("db_structure");
			if( prop.getProperty("application_homedir") != null)
				this.application_homedir = prop.getProperty("application_homedir");
			if( prop.getProperty("application_datadir") != null)
				this.application_datadir = prop.getProperty("application_datadir");
			if( prop.getProperty("upload_homedir") != null)
				this.upload_homedir = prop.getProperty("upload_homedir");
			if( prop.getProperty("db_application_url") != null)
				this.db_application_url = prop.getProperty("db_application_url");
			if( prop.getProperty("db_application_username") != null)
				this.db_application_username = prop.getProperty("db_application_username");
			if( prop.getProperty("db_application_password") != null)
				this.db_application_password = prop.getProperty("db_application_password");
			if( prop.getProperty("db_osm2pgsql_url") != null)
				this.db_osm2pgsql_url = prop.getProperty("db_osm2pgsql_url");
			if( prop.getProperty("db_osm2pgsql_username") != null)
				this.db_osm2pgsql_username = prop.getProperty("db_osm2pgsql_username");
			if( prop.getProperty("db_osm2pgsql_password") != null)
				this.db_osm2pgsql_password = prop.getProperty("db_osm2pgsql_password");
			if( prop.getProperty("db_osm2pgsqlwrite_username") != null)
				this.db_osm2pgsqlwrite_username = prop.getProperty("db_osm2pgsqlwrite_username");
			if( prop.getProperty("db_osm2pgsqlwrite_password") != null)
				this.db_osm2pgsqlwrite_password = prop.getProperty("db_osm2pgsqlwrite_password");


			if( prop.getProperty("osmosis_laststatefile") != null)
				this.osmosis_laststatefile = prop.getProperty("osmosis_laststatefile");
			if( prop.getProperty("logging_filename") != null)
				this.logging_filename = prop.getProperty("logging_filename");
			if( prop.getProperty("logging_console_level") != null)
				this.logging_console_level = Level.parse(prop.getProperty("logging_console_level"));
			if( prop.getProperty("logging_file_level") != null)
				this.logging_file_level = Level.parse(prop.getProperty("logging_file_level"));

			if(debugoutput) {
				System.out.println(" .servername                              ==="+this.servername+"===");
				System.out.println(" .application_homedir                     ==="+this.application_homedir+"===");
				System.out.println(" .application_datadir                     ==="+this.application_datadir+"===");
				System.out.println(" .upload_homedir                          ==="+this.upload_homedir+"===");
				System.out.println(" .db_application_url                      ==="+this.db_application_url+"===");
				System.out.println(" .db_application_username                 ==="+this.db_application_username+"===");
				System.out.println(" .db_application_password                 ==="+this.db_application_password+"===");
				System.out.println(" .db_osm2pgsql_url                        ==="+this.db_osm2pgsql_url+"===");
				System.out.println(" .db_osm2pgsql_username                   ==="+this.db_osm2pgsql_username+"===");
				System.out.println(" .db_osm2pgsql_password                   ==="+this.db_osm2pgsql_password+"===");
				System.out.println(" .db_osm2pgsqlwrite_username              ==="+this.db_osm2pgsqlwrite_username+"===");
				System.out.println(" .db_osm2pgsqlwrite_password              ==="+this.db_osm2pgsqlwrite_password+"===");
				System.out.println(" .osmosis_laststatefile                   ==="+this.osmosis_laststatefile+"===");
				System.out.println(" .logging_filename                        ==="+this.logging_filename +"===");
				System.out.println(" .logging_console_level                   ==="+this.logging_console_level.toString() +"===");
				System.out.println(" .logging_file_level                      ==="+this.logging_file_level.toString() +"===");
			}

		} catch (Exception e) {
			System.out.println("ERROR: failed to read file ==="+configuration_filename+"===");
			e.printStackTrace();
			return;
		}
	}
}

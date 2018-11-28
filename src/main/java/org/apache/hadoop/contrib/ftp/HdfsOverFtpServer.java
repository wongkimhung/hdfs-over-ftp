package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Start-up class of FTP server
 */
public class HdfsOverFtpServer {

	private static Logger log = Logger.getLogger(HdfsOverFtpServer.class);

	private static int port = 0;
	private static int sslPort = 0;
	private static String passivePorts = null;
	private static String sslPassivePorts = null;
	private static String hdfsUri = null;

	public static void main(String[] args) throws Exception {
		loadConfig();

		if (port != 0) {
			startServer();
		}

		if (sslPort != 0) {
			startSSLServer();
		}
	}

	/**
	 * Load configuration
	 *
	 * @throws IOException
	 */
	private static void loadConfig() throws IOException {
		Properties props = new Properties();
		props.load(loadResource("/hdfs-over-ftp.properties"));

		try {
			port = Integer.parseInt(props.getProperty("port"));
			log.info("port is set. ftp server will be started");
		} catch (Exception e) {
			log.info("port is not set. so ftp server will not be started");
		}

		try {
			sslPort = Integer.parseInt(props.getProperty("ssl-port"));
			log.info("ssl-port is set. ssl server will be started");
		} catch (Exception e) {
			log.info("ssl-port is not set. so ssl server will not be started");
		}

		if (port != 0) {
			passivePorts = props.getProperty("data-ports");
			if (passivePorts == null) {
				log.fatal("data-ports is not set");
				System.exit(1);
			}
		}

		if (sslPort != 0) {
			sslPassivePorts = props.getProperty("ssl-data-ports");
			if (sslPassivePorts == null) {
				log.fatal("ssl-data-ports is not set");
				System.exit(1);
			}
		}

		hdfsUri = props.getProperty("hdfs-uri");
		if (hdfsUri == null) {
			log.fatal("hdfs-uri is not set");
			System.exit(1);
		}

		String superuser = props.getProperty("superuser");
		if (superuser == null) {
			log.fatal("superuser is not set");
			System.exit(1);
		}
		HdfsOverFtpSystem.setSuperuser(superuser);
	}

	/**
	 * Starts FTP server
	 *
	 * @throws Exception
	 */
	public static void startServer() throws Exception {

		log.info(
				"Starting Hdfs-Over-Ftp server. port: " + port + " data-ports: " + passivePorts + " hdfs-uri: " + hdfsUri);

		HdfsOverFtpSystem.setHDFS_URI(hdfsUri);
		
		HdfsUserManager userManager = new HdfsUserManager();
		final InputStream file = loadResource("/users.properties");

		userManager.setFile(file);

		FtpServerFactory serverFactory = new FtpServerFactory();
		serverFactory.setFileSystem(new HdfsFileSystemManager());
		serverFactory.setUserManager(userManager);
		
        ListenerFactory listenerFactory = new ListenerFactory();
        
		DataConnectionConfigurationFactory dataConFactory = new DataConnectionConfigurationFactory();
		dataConFactory.setPassivePorts(passivePorts);
		
		listenerFactory.setPort(port);
		listenerFactory.setDataConnectionConfiguration(dataConFactory.createDataConnectionConfiguration());
        
		serverFactory.addListener("default", listenerFactory.createListener());
		FtpServer server = serverFactory.createServer();

		server.start();
	}

	private static InputStream loadResource(String resourceName) {
		final InputStream in = HdfsOverFtpServer.class.getResourceAsStream(resourceName);
		if (in == null) {
			throw new RuntimeException("Resource not found: " + resourceName);
		}
		return in;
	}

	/**
	 * Starts SSL FTP server
	 *
	 * @throws Exception
	 */
	public static void startSSLServer() throws Exception {

		log.info(
				"Starting Hdfs-Over-Ftp SSL server. ssl-port: " + sslPort + " ssl-data-ports: " + sslPassivePorts + " hdfs-uri: " + hdfsUri);


		HdfsOverFtpSystem.setHDFS_URI(hdfsUri);

		HdfsUserManager userManager = new HdfsUserManager();
		userManager.setFile(new FileInputStream("users.conf"));

		FtpServerFactory serverFactory = new FtpServerFactory();
		serverFactory.setFileSystem(new HdfsFileSystemManager());
		serverFactory.setUserManager(userManager);

        ListenerFactory listenerFactory = new ListenerFactory();
        
		DataConnectionConfigurationFactory dataConFactory = new DataConnectionConfigurationFactory();
		dataConFactory.setPassivePorts(sslPassivePorts);
		
		MySslConfiguration ssl = new MySslConfiguration();
		ssl.setKeystoreFile(new File("ftp.jks"));
		ssl.setKeystoreType("JKS");
		ssl.setKeyPassword("333333");
		
		listenerFactory.setPort(sslPort);
		listenerFactory.setDataConnectionConfiguration(dataConFactory.createDataConnectionConfiguration());
		listenerFactory.setImplicitSsl(true);
		listenerFactory.setSslConfiguration(ssl);
		
		serverFactory.addListener("default", listenerFactory.createListener());
		
		FtpServer server = serverFactory.createServer();

		server.start();
	}
}

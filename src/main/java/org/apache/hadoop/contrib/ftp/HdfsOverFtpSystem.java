package org.apache.hadoop.contrib.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Class to store DFS connection
 */
public class HdfsOverFtpSystem {

	private static FileSystem dfs = null;

	public static String HDFS_URI = "";

	private static String superuser = "error";
	private static String supergroup = "supergroup";

	private final static Logger log = LoggerFactory.getLogger(HdfsOverFtpSystem.class);


	private static void hdfsInit() throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", superuser + "," + supergroup);
		try {
			conf.set("fs.defaultFS", "hdfs://192.168.1.120:9000");
			dfs = FileSystem.get(URI.create(HDFS_URI), conf, superuser);
		} catch (Exception e) {
			log.error("DFS Initialization error", e);
		}
	}

	public static void setHDFS_URI(String HDFS_URI) {
		HdfsOverFtpSystem.HDFS_URI = HDFS_URI;
	}

	/**
	 * Get dfs
	 *
	 * @return dfs
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static FileSystem getDfs() throws IOException, InterruptedException {
		if (dfs == null) {
			hdfsInit();
		}
		return dfs;
	}

	/**
	 * Set superuser. and we connect to DFS as a superuser
	 *
	 * @param superuser
	 */
	public static void setSuperuser(String superuser) {
		HdfsOverFtpSystem.superuser = superuser;
	}

//  public static String dirList(String path) throws IOException {
//    String res = "";
//
//        getDfs();
//
//        Path file = new Path(path);
//        FileStatus fileStats[] = dfs.listStatus(file);
//
//        for (FileStatus fs : fileStats) {
//            if (fs.isDir()) {
//                res += "d";
//            } else {
//                res += "-";
//            }
//
//            res += fs.getPermission();
//            res += " 1";
//            res += " " + fs.getOwner();
//            res += " " + fs.getGroup();
//            res += " " + fs.getLen();
//            res += " " + new Date(fs.getModificationTime()).toString().substring(4, 16);
//            res += " " + fs.getPath().getName();
//            res += "\n";
//        }
//    return res;
//  }
}

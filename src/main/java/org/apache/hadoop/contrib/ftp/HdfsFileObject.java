package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements all actions to HDFS
 */
public class HdfsFileObject implements FtpFile {

	private final Logger log = LoggerFactory.getLogger(HdfsFileObject.class);

	private Path path;
	private HdfsUser user;

	/**
	 * Constructs HdfsFtpFile from path
	 *
	 * @param path path to represent object
	 * @param user accessor of the object
	 */
	public HdfsFileObject(String path, User user) {
		this.path = new Path(path);
		this.user = (HdfsUser) user;
	}

	/**
	 * Get full name of the object
	 *
	 * @return full name of the object
	 */
	@Override
	public String getAbsolutePath() {
		return path.toString();
	}

	/**
	 * Get short name of the object
	 *
	 * @return short name of the object
	 */
	@Override
	public String getName() {
		String full = getAbsolutePath();
		int pos = full.lastIndexOf("/");
		if (pos == 0) {
			return "/";
		}
		return full.substring(pos + 1);
	}

	/**
	 * HDFS has no hidden objects
	 *
	 * @return always false
	 */
	@Override
	public boolean isHidden() {
		return false;
	}

	/**
	 * Checks if the object is a directory
	 *
	 * @return true if the object is a directory
	 */
	@Override
	public boolean isDirectory() {
		try {
			log.debug("is directory? : " + path);
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			return fs.isDirectory();
		} catch (Exception e) {
			log.debug(path + " is not dir", e);
			return false;
		}
	}

	/**
	 * Get HDFS permissions
	 *
	 * @return HDFS permissions as a FsPermission instance
	 * @throws IOException if path doesn't exist so we get permissions of parent object in that case
	 */
	private FsPermission getPermissions() throws Exception {
		FileSystem dfs = HdfsOverFtpSystem.getDfs();
		return dfs.getFileStatus(path).getPermission();
	}

	/**
	 * Checks if the object is a file
	 *
	 * @return true if the object is a file
	 */
	@Override
	public boolean isFile() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			return dfs.isFile(path);
		} catch (Exception e) {
			log.debug(path + " is not file", e);
			return false;
		}
	}

	/**
	 * Checks if the object does exist
	 *
	 * @return true if the object does exist
	 */
	@Override
	public boolean doesExist() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.getFileStatus(path);
			return true;
		} catch (Exception e) {
			//   log.debug(path + " does not exist", e);
			return false;
		}
	}

	/**
	 * Checks if the user has a read permission on the object
	 *
	 * @return true if the user can read the object
	 */
	@Override
	public boolean isReadable() {
		try {
			FsPermission permissions = getPermissions();
			if (user.getName().equals(getOwnerName())) {
				if (permissions.toString().substring(0, 1).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for user");
					return true;
				}
			} else if (user.isGroupMember(getGroupName())) {
				if (permissions.toString().substring(3, 4).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for group");
					return true;
				}
			} else {
				if (permissions.toString().substring(6, 7).equals("r")) {
					log.debug("PERMISSIONS: " + path + " - " + " read allowed for others");
					return true;
				}
			}
			log.debug("PERMISSIONS: " + path + " - " + " read denied");
			return false;
		} catch (Exception e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
			return false;
		}
	}

	private HdfsFileObject getParent() {
		String pathS = path.toString();
		String parentS = "/";
		int pos = pathS.lastIndexOf("/");
		if (pos > 0) {
			parentS = pathS.substring(0, pos);
		}
		return new HdfsFileObject(parentS, user);
	}

	/**
	 * Checks if the user has a write permission on the object
	 *
	 * @return true if the user has write permission on the object
	 */
	@Override
	public boolean isWritable() {
		try {
			FsPermission permissions = getPermissions();
			if (user.getName().equals(getOwnerName())) {
				if (permissions.toString().substring(1, 2).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for user");
					return true;
				}
			} else if (user.isGroupMember(getGroupName())) {
				if (permissions.toString().substring(4, 5).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for group");
					return true;
				}
			} else {
				if (permissions.toString().substring(7, 8).equals("w")) {
					log.debug("PERMISSIONS: " + path + " - " + " write allowed for others");
					return true;
				}
			}
			log.debug("PERMISSIONS: " + path + " - " + " write denied");
			return false;
		} catch (Exception e) {
			return getParent().isWritable();
		}
	}

	/**
	 * Checks if the user has a delete permission on the object
	 *
	 * @return true if the user has delete permission on the object
	 */
	@Override
	public boolean isRemovable() {
		return isWritable();
	}

	/**
	 * Get owner of the object
	 *
	 * @return owner of the object
	 */
	@Override
	public String getOwnerName() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			return fs.getOwner();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get group of the object
	 *
	 * @return group of the object
	 */
	@Override
	public String getGroupName() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			return fs.getGroup();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Get link count
	 *
	 * @return 3 is for a directory and 1 is for a file
	 */
	@Override
	public int getLinkCount() {
		return isDirectory() ? 3 : 1;
	}

	/**
	 * Get last modification date
	 *
	 * @return last modification date as a long
	 */
	@Override
	public long getLastModified() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			return fs.getModificationTime();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * Get a size of the object
	 *
	 * @return size of the object in bytes
	 */
	@Override
	public long getSize() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(path);
			log.info("getSize(): " + path + " : " + fs.getLen());
			return fs.getLen();
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * Create a new dir from the object
	 *
	 * @return true if dir is created
	 */
	@Override
	public boolean mkdir() {
		if (!isWritable()) {
			log.debug("No write permission : " + path);
			return false;
		}

		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			return dfs.mkdirs(path);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Delete object from the HDFS filesystem
	 *
	 * @return true if the object is deleted
	 */
	@Override
	public boolean delete() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			return dfs.delete(path, true);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Move the object to another location
	 *
	 * @param FtpFile location to move the object
	 * @return true if the object is moved successfully
	 */
	@Override
	public boolean move(FtpFile FtpFile) {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.rename(path, new Path(FtpFile.getAbsolutePath()));
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * List files of the directory
	 *
	 * @return List of files in the directory
	 */
	@Override
	public List<? extends FtpFile> listFiles() {

		if (!isReadable()) {
			log.debug("No read permission : " + path);
			return null;
		}

		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fileStats[] = dfs.listStatus(path);

			List<FtpFile> FtpFiles = new ArrayList<FtpFile>();
			for (int i = 0; i < fileStats.length; i++) {
				FtpFiles.add(new HdfsFileObject(fileStats[i].getPath().toString(), user));
			}
			return FtpFiles;
		} catch (Exception e) {
			log.debug("", e);
			return null;
		}
	}

	/**
	 * Creates output stream to write to the object
	 *
	 * @param l is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	@Override
	public OutputStream createOutputStream(long l) throws IOException {
		// permission check
		if (!isWritable()) {
			throw new IOException("No write permission : " + path);
		}

		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FSDataOutputStream out = dfs.create(path);
			dfs.setOwner(path, user.getName(), user.getMainGroup());
			return out;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates input stream to read from the object
	 *
	 * @param l is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	@Override
	public InputStream createInputStream(long l) throws IOException {
		// permission check
		if (!isReadable()) {
			throw new IOException("No read permission : " + path);
		}
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FSDataInputStream in = dfs.open(path);
			return in;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public boolean setLastModified(long time) {
		return false;
	}

	@Override
	public Object getPhysicalFile() {
		return path;
	}
}

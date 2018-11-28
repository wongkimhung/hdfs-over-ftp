package org.apache.hadoop.contrib.ftp;

import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.AuthorizationRequest;
import org.apache.ftpserver.ftplet.User;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Implemented User to add group persmissions
 */
public class HdfsUser implements User, Serializable {

	private static final long serialVersionUID = -47371353779731294L;

	private String name = null;

	private String password = null;

	private int maxIdleTimeSec = 0; // no limit

	private String homeDir = null;

	private boolean isEnabled = true;

	private List<? extends Authority> authorities = new ArrayList<Authority>();

	private ArrayList<String> groups = new ArrayList<String>();

	private Logger log = Logger.getLogger(HdfsUser.class);

	/**
	 * Default constructor.
	 */
	public HdfsUser() {
	}

	/**
	 * Copy constructor.
	 */
	public HdfsUser(User user) {
		name = user.getName();
		password = user.getPassword();
		authorities = user.getAuthorities();
		maxIdleTimeSec = user.getMaxIdleTime();
		homeDir = user.getHomeDirectory();
		isEnabled = user.getEnabled();
	}

	public ArrayList<String> getGroups() {
		return groups;
	}

	/**
	 * Get the main group of the user
	 *
	 * @return main group of the user
	 */
	public String getMainGroup() {
		if (groups.size() > 0) {
			return groups.get(0);
		} else {
			log.error("User " + name + " is not a memer of any group");
			return "error";
		}
	}

	/**
	 * Checks if user is a member of the group
	 *
	 * @param group to check
	 * @return true if the user id a member of the group
	 */
	public boolean isGroupMember(String group) {
		for (String userGroup : groups) {
			if (userGroup.equals(group)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Set users' groups
	 *
	 * @param groups to set
	 */
	public void setGroups(ArrayList<String> groups) {
		if (groups.size() < 1) {
			log.error("User " + name + " is not a memer of any group");
		}
		this.groups = groups;
	}

	/**
	 * Get the user name.
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * Set user name.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Get the user password.
	 */
	@Override
	public String getPassword() {
		return password;
	}

	/**
	 * Set user password.
	 */
	public void setPassword(String pass) {
		password = pass;
	}

	@Override
	public List<? extends Authority> getAuthorities() {
		if (authorities != null) {
			return authorities;
		} else {
			return null;
		}
	}

	public void setAuthorities(List<Authority> authorities) {
		if (authorities != null) {
			this.authorities = authorities;
		} else {
			this.authorities = null;
		}
	}

	/**
	 * Get the maximum idle time in second.
	 */
	@Override
	public int getMaxIdleTime() {
		return maxIdleTimeSec;
	}

	/**
	 * Set the maximum idle time in second.
	 */
	public void setMaxIdleTime(int idleSec) {
		maxIdleTimeSec = idleSec;
		if (maxIdleTimeSec < 0) {
			maxIdleTimeSec = 0;
		}
	}

	/**
	 * Get the user enable status.
	 */
	@Override
	public boolean getEnabled() {
		return isEnabled;
	}

	/**
	 * Set the user enable status.
	 */
	public void setEnabled(boolean enb) {
		isEnabled = enb;
	}

	/**
	 * Get the user home directory.
	 */
	@Override
	public String getHomeDirectory() {
		return homeDir;
	}

	/**
	 * Set the user home directory.
	 */
	public void setHomeDirectory(String home) {
		homeDir = home;
	}

	/**
	 * String representation.
	 */
	@Override
	public String toString() {
		return name;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AuthorizationRequest authorize(AuthorizationRequest request) {
		List<? extends Authority> authorities = getAuthorities();

		// check for no authorities at all
		if (authorities == null) {
			return null;
		}

		boolean someoneCouldAuthorize = false;
		for (int i = 0; i < authorities.size(); i++) {
			Authority authority = authorities.get(i);

			if (authority.canAuthorize(request)) {
				someoneCouldAuthorize = true;

				request = authority.authorize(request);

				// authorization failed, return null
				if (request == null) {
					return null;
				}
			}

		}

		if (someoneCouldAuthorize) {
			return request;
		} else {
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Authority> getAuthorities(Class<? extends Authority> clazz) {
		List<Authority> selected = new ArrayList<Authority>();

		for (int i = 0; i < authorities.size(); i++) {
			if (authorities.get(i).getClass().equals(clazz)) {
				selected.add(authorities.get(i));
			}
		}

		return selected;
	}
}

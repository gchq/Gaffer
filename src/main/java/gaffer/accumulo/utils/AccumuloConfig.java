/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.accumulo.utils;

import java.io.*;
import java.util.Properties;

import org.apache.hadoop.io.IOUtils;

/**
 * A simple wrapper around a Java Properties class with some convenience methods
 * for common properties required for connecting to an Accumulo instance.
 */
public class AccumuloConfig implements Serializable {

    private static final long serialVersionUID = -4479798591872551929L;

	public final static String INSTANCE_NAME = "accumulo.instance";
	public final static String ZOOKEEPERS = "accumulo.zookeepers";
	public final static String TABLE = "accumulo.table";
	public final static String USER = "accumulo.user";
	public final static String PASSWORD = "accumulo.password";
	public final static String AGE_OFF_TIME_IN_DAYS = "accumulo.ageofftimeindays";

    // If the zookeeper string in the AccumuloConfig equals the following
    // value then we are using a MockAccumulo instance.
    public static final String MOCK_ZOOKEEPERS = "null";

    private Properties props = null;
    private String propFileLocation = null;

    /**
     * Create an AccumuloConfig object, which is a simple wrapper around a Java
     * Properties class with some convenience methods for common properties.
     */
    public AccumuloConfig() {
    	this.props = new Properties();
    }

    /**
     * Create an AccumuloConfig object, which is a simple wrapper around a Java
     * Properties class with some convenience methods for common properties.
     * 
     * @param propFileLocation
     *            location of the properties file. Will check the classpath first, 
     *            i.e. if it is embedded in the Jar, otherwise will try to find 
     *            it in the local file system.
     */
    public AccumuloConfig(String propFileLocation) {
        this.propFileLocation = propFileLocation;
    }

    private void readProperties() {
        InputStream accIs = getClass().getResourceAsStream(propFileLocation);
        try {
        	if (accIs != null) {
        		props = new Properties();
        		props.load(accIs);
        	}
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeStream(accIs);
        }
        
        if (props == null) {
        	// Properties file was not found in the jar, check if it is
        	// present in the local file system
        	FileInputStream inputStream = null;
        	try {
				inputStream = new FileInputStream(propFileLocation);
				props = new Properties();
				props.load(inputStream);
			} catch (FileNotFoundException e) {
				throw new RuntimeException(String.format("Could not find properties file %s", propFileLocation), e);
			} catch (IOException e) {
                throw new RuntimeException(String.format("Could not find properties file %s", propFileLocation), e);
			} catch (Exception e) {
                throw new RuntimeException(String.format("Error occurred while trying to read properties file %s:",
                        propFileLocation), e);
			} finally {
				if (inputStream != null) {
					IOUtils.closeStream(inputStream);
				}
			}
        }
    }

    /**
     * Get a parameter from the config file.
     * 
     * @param key  The name of the parameter
     * @return The value of the parameter
     */
    public String get(String key) {
        synchronized (this) {
            if (props == null) {
                readProperties();
            }
        }
        return props.getProperty(key);
    }

    /**
     * Get a parameter from the config file, or the default value.
     * 
     * @param key  The name of the parameter
     * @param defaultValue  The value to return if the key is not present
     * @return The value of the parameter if it exists, otherwise the default value
     */
    public String get(String key, String defaultValue) {
        synchronized (this) {
            if (props == null) {
                readProperties();
            }
        }
        return props.getProperty(key, defaultValue);
    }

    /**
     * Set a parameter from the config file.
     *
     * @param key
     * @param value
     */
    public void set(String key, String value) {
        synchronized (this) {
            if (props == null) {
                readProperties();
            }
        }
        props.setProperty(key, value);
    }

    /**
     * Get the list of Zookeeper servers.
     * 
     * @return
     */
    public String getZookeepers() {
        return get(ZOOKEEPERS);
    }

    /**
     * Set the list of Zookeeper servers.
     * @param zookeepers
     */
    public void setZookeepers(String zookeepers) {
    	set(ZOOKEEPERS, zookeepers);
    }

    /**
     * Get the Accumulo instance name.
     * 
     * @return
     */
    public String getInstanceName() {
        return get(INSTANCE_NAME);
    }

    /**
     * Set the Accumulo instance name.
     * @param instanceName
     */
    public void setInstanceName(String instanceName) {
        set(INSTANCE_NAME, instanceName);
    }

    /**
     * Get the particular table name.
     * 
     * @return
     */
    public String getTable() {
        return get(TABLE);
    }

    /**
     * Set the table name.
     * @param tableName
     */
    public void setTable(String tableName) {
        set(TABLE, tableName);
    }

    /**
     * Get the configured Accumulo user.
     * 
     * @return
     */
    public String getUserName() {
        return get(USER);
    }

    /**
     * Set the configured Accumulo user.
     * @param userName
     */
    public void setUserName(String userName) {
        set(USER, userName);
    }

    /**
     * Get the password for the Accumulo user.
     * 
     * @return
     */
    public String getPassword() {
        return get(PASSWORD);
    }

    /**
     * Set the password to use for the Accumulo user.
     * @param password
     */
    public void setPassword(String password) {
        set(PASSWORD, password);
    }

    /**
     * Get the age off time in days.
     * @return
     * @throws Exception
     */
    public Integer getAgeOffTimeInDays() throws Exception {
    	String ageOffTimeTemp = get(AGE_OFF_TIME_IN_DAYS);
    	if (ageOffTimeTemp == null) {
    		throw new Exception("Unable to find the " + AGE_OFF_TIME_IN_DAYS + " property in the accumulo config file");
    	}

    	Integer ageOffTimeInDays = null;
    	try {
    		ageOffTimeInDays = Integer.parseInt(ageOffTimeTemp);
    	} catch (NumberFormatException e) {
    		throw new Exception("Unable to parse the value of the " + AGE_OFF_TIME_IN_DAYS + " property in the accumulo config file, the age off needs to be specified in days, e.g. " + AGE_OFF_TIME_IN_DAYS + "=180");
    	}

        if (ageOffTimeInDays <= 0) {
        	throw new Exception("Invalid value for the " + AGE_OFF_TIME_IN_DAYS + " property in the accumulo config file, it needs to be a positive integer, value given was: " + ageOffTimeInDays);
        }

    	return ageOffTimeInDays;
    }

    /**
     * Set the age off time in days.
     */
    public void setAgeOffTimeInDays(int ageOffTimeInDays) {
    	this.props.setProperty(AGE_OFF_TIME_IN_DAYS, String.valueOf(ageOffTimeInDays));
    }

    /**
     * Get the age off time in milliseconds
     * @return
     * @throws Exception
     */
    public Long getAgeOffTimeInMilliseconds() throws Exception {
    	Integer ageOffTimeInDays = this.getAgeOffTimeInDays();
    	return ageOffTimeInDays * 24 * 60 * 60 * 1000L;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        if (props == null) {
            readProperties();
        }
        out.defaultWriteObject();
    }
}
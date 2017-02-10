/*
 * Copyright 2016-2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.hbasestore;

import org.apache.hadoop.hbase.TableName;
import uk.gov.gchq.gaffer.store.StoreProperties;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * HBaseProperties contains specific configuration information for the
 * hbase store, such as database connection strings. It wraps
 * {@link uk.gov.gchq.gaffer.data.element.Properties} and lazy loads the all properties from
 * a file when first used.
 */
public class HBaseProperties extends StoreProperties {
    public static final String INSTANCE_NAME = "hbase.instance";
    public static final String ZOOKEEPERS = "hbase.zookeepers";
    public static final String TABLE = "hbase.table";
    public static final String USER = "hbase.user";
    public static final String PASSWORD = "hbase.password";

    public HBaseProperties() {
        super();
    }

    public HBaseProperties(final Path propFileLocation) {
        super(propFileLocation);
    }

    public static HBaseProperties loadStoreProperties(final InputStream storePropertiesStream) {
        return ((HBaseProperties) StoreProperties.loadStoreProperties(storePropertiesStream));
    }

    @Override
    public HBaseProperties clone() {
        return (HBaseProperties) super.clone();
    }

    /**
     * Get the list of Zookeeper servers.
     *
     * @return A comma separated list of Zookeeper servers
     */
    public String getZookeepers() {
        return get(ZOOKEEPERS);
    }

    /**
     * Set the list of Zookeeper servers.
     *
     * @param zookeepers the list of Zookeeper servers
     */
    public void setZookeepers(final String zookeepers) {
        set(ZOOKEEPERS, zookeepers);
    }

    /**
     * Get the HBase instance name.
     *
     * @return Return the instance name of hbase set in the properties file
     */
    public String getInstanceName() {
        return get(INSTANCE_NAME);
    }

    /**
     * Set the HBase instance name.
     *
     * @param instanceName the HBase instance name
     */
    public void setInstanceName(final String instanceName) {
        set(INSTANCE_NAME, instanceName);
    }

    /**
     * Get the particular table name.
     *
     * @return The hbase table to use as set in the properties file
     */
    public TableName getTableName() {
        return TableName.valueOf(get(TABLE));
    }

    /**
     * Set the table name.
     *
     * @param tableName the table name
     */
    public void setTable(final String tableName) {
        set(TABLE, tableName);
    }

    /**
     * Get the configured HBase user.
     *
     * @return Get the configured hbase user
     */
    public String getUserName() {
        return get(USER);
    }

    /**
     * Set the configured HBase user.
     *
     * @param userName the configured HBase user
     */
    public void setUserName(final String userName) {
        set(USER, userName);
    }

    /**
     * Get the password for the HBase user.
     *
     * @return the password for the configured hbase user
     */
    public String getPassword() {
        return get(PASSWORD);
    }

    /**
     * Set the password to use for the HBase user.
     *
     * @param password the password to use for the HBase user
     */
    public void setPassword(final String password) {
        set(PASSWORD, password);
    }
}

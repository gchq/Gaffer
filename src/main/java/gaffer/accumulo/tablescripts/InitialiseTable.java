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
package gaffer.accumulo.tablescripts;

import gaffer.accumulo.TableUtils;
import gaffer.accumulo.splitpoints.EstimateSplitPointsDriver;
import gaffer.accumulo.utils.Accumulo;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.accumulo.utils.IngestUtils;

import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Creates a table in Accumulo ready for the insertion of Gaffer data. This sets
 * up the correct iterators, the Bloom filter, etc. It requires the splits to be
 * provided - these should be estimated using {@link EstimateSplitPointsDriver}.
 */
public class InitialiseTable extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		// Usage message
		if (args.length != 2) {
			System.err.println("Usage: accumulo_properties_file splits_file");
			System.err.println("Properties file should contain the following parameters:");
			System.err.println("\t" + AccumuloConfig.INSTANCE_NAME);
			System.err.println("\t" + AccumuloConfig.ZOOKEEPERS);
			System.err.println("\t" + AccumuloConfig.TABLE);
			System.err.println("\t" + AccumuloConfig.USER);
			System.err.println("\t" + AccumuloConfig.PASSWORD);
			System.err.println("\t" + AccumuloConfig.AGE_OFF_TIME_IN_DAYS);
			return 1;
		}

		// Get parameters
		String propertyFilename = args[0];
		String splitsFilename = args[1];
		
		// Create AccumuloConfig from property file
		AccumuloConfig accumuloConfig = new AccumuloConfig(propertyFilename);
		
		// Get connector to Accumulo
		Connector connector = Accumulo.connect(accumuloConfig);
		System.out.println("Connected to Accumulo");
		
		// Get table name - throw error if that table already exists
		String tableName = accumuloConfig.getTable();
        if (connector.tableOperations().exists(tableName)) {
            System.err.printf("Table %s already exists - specify new table name", tableName);
            System.exit(1);
        } else {
        	System.out.println("Confirmed that table " + tableName + " doesn't already exist.");
        }

        // Get the age off time in days from the config
        Long ageOffTimeInMilliseconds = accumuloConfig.getAgeOffTimeInMilliseconds();
        System.out.println("Age off time in days will be set to " + accumuloConfig.getAgeOffTimeInDays());

        // Create the table
        try {
			TableUtils.createTable(connector, tableName, ageOffTimeInMilliseconds);
		} catch (AccumuloException e) {
			System.err.println("Exception creating table " + e);
			return 1;
		} catch (AccumuloSecurityException e) {
			System.err.println("Exception creating table " + e);
			return 1;
		} catch (TableExistsException e) {
			System.err.println("Exception creating table " + e);
			return 1;
		} catch (TableNotFoundException e) {
			System.err.println("Exception creating table " + e);
			return 1;
		}
        System.out.println("Table " + tableName + " successfully created.");
        
        // Hadoop conf
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        
        // Set the splits on the table
        SortedSet<Text> splits = IngestUtils.getSplitsFromFile(fs, new Path(splitsFilename));
        connector.tableOperations().addSplits(tableName, splits);
        System.out.println("Setting splits on the table from splits file " + splitsFilename);
        
        return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new InitialiseTable(), args);
		System.exit(exitCode);
	}

}

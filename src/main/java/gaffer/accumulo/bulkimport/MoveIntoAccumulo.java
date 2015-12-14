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
package gaffer.accumulo.bulkimport;

import gaffer.accumulo.utils.Accumulo;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.accumulo.utils.IngestUtils;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Class to take data produced by {@link BulkImportDriver} and move it into Accumulo.
 * It requires three parameters: the data path, a directory for any failures to be moved
 * into, and an Accumulo properties file. (See the javadoc for {@link BulkImportDriver}
 * for information about what should be in the Accumulo properties file.)
 */
public class MoveIntoAccumulo extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// Usage
		if (args.length < 3) {
			System.err.println("Usage: " + MoveIntoAccumulo.class.getName() + " <inputpath> <failurepath> <accumulo_properties_file>");
			return 1;
		}

		// Gets paths
		Path inputPath = new Path(args[0]);
		Path failurePath = new Path(args[1]);
		String accumuloPropertiesFile = args[2];

		// Hadoop configuration
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		// Connect to Accumulo
		AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFile);
		Connector conn = Accumulo.connect(accConf);
		String tableName = accConf.getTable();

		// Check if the table exists
		if (!conn.tableOperations().exists(tableName)) {
			System.err.println("Table " + tableName + " does not exist - create the table before running this");
			return 1;
		}

		// Make the failure directory
		fs.mkdirs(failurePath);
		fs.setPermission(failurePath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

		// Remove the _SUCCESS file to prevent warning in accumulo
		fs.delete(new Path(inputPath + "/_SUCCESS"), false);

		// Set all permissions
		IngestUtils.setDirectoryPermsForAccumulo(fs, inputPath);

		// Import the files
		conn.tableOperations().importDirectory(tableName, inputPath.toString(), failurePath.toString(), false);

		// Delete the temporary directories
		fs.delete(failurePath, true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MoveIntoAccumulo(), args);
		System.exit(exitCode);
	}

}

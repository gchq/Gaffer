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
package gaffer.accumulo.inputformat.example;

import gaffer.graph.TypeValue;
import geotrellis.spark.io.accumulo.BatchAccumuloInputFormat;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.inputformat.BatchScannerElementInputFormat;
import gaffer.accumulo.inputformat.ElementInputFormat;
import gaffer.accumulo.utils.Accumulo;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.graph.Edge;
import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * An example of a driver class that runs a MapReduce job over an Accumulo table
 * containing Gaffer data. It allows the user to specify the time window of interest
 * and whether they want the data to be rolled up over time and visibility. It also
 * allows the user to specify a file of {@link TypeValue}s of interest, in which
 * case only data involving those will be returned.
 * 
 * If the user does not specify any {@link TypeValue}s then an iterator is applied
 * which ensures that each {@link Edge} is only seen once.
 *
 * Either {@link ElementInputFormat} or {@link BatchScannerElementInputFormat} is used.
 * The former is based on Accumulo's standard {@link InputFormat} and is used for full
 * table scans; the latter is based on {@link BatchAccumuloInputFormat} and is used when
 * seeds have been specified.
 */
public class ExampleDriver extends Configured implements Tool {

	private final static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}
	
	private static final String USAGE = "Usage: " + ExampleDriver.class.getName()
			+ " <output_path> <accumulo_properties_file> <num_reduce_tasks> <startDate as yyyyMMdd or null>"
			+ " <endDate as yyyyMMdd or null> <roll_up_over_time_and_visibility - true or false>"
			+ " <optional - file of type-values>\n"
			+ "Note that entities and edges whose start and end date fit within the specified time window are "
			+ "returned - the start of the specified time window is midnight at the beginning of the given start date "
			+ "and the end of the window is midnight at the beginning of the given end date. "
			+ "So to run over April 1st 2014 use a start date of 20140401 and an end date of 20140402. "
			+ "If a file of type-values is specified it should have one per line in the form type|value.";
	
	public int run(String[] args) throws Exception {
		// Usage
		if (args.length != 6 && args.length != 7) {
			System.err.println(USAGE);
			return 1;
		}

		// Parse options
		Path outputPath = new Path(args[0]);
		String accumuloPropertiesFile = args[1];
		int numReduceTasks;
		try {
			numReduceTasks = Integer.parseInt(args[2]);
		} catch (NumberFormatException e) {
			System.err.println(USAGE);
			return 1;
		}
		Date startDate = null;
		Date endDate = null;
		boolean useTimeWindow = false;
		if (!args[3].equals("null") && !args[4].equals("null")) {
			try {
				startDate = DATE_FORMAT.parse(args[3]);
				endDate = DATE_FORMAT.parse(args[4]);
			} catch (ParseException e) {
				System.err.println("Error parsing dates: " + args[3] + " " + args[4]
						+ " " + e.getMessage());
				return 1;
			}
			useTimeWindow = true;
		}
		boolean rollUpOverTimeAndVisibility = Boolean.parseBoolean(args[5]);
		boolean seedsSpecified = (args.length == 7);
		String seedsFile = "";
		if (seedsSpecified) {
			seedsFile = args[6];
		}
		
		// Hadoop configuration
		Configuration conf = getConf();
		
		// Connect to Accumulo, so we can check connection and check that the
		// table exists
		AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFile);
		Connector conn = Accumulo.connect(accConf);
		String tableName = accConf.getTable();
		Authorizations authorizations = conn.securityOperations().getUserAuthorizations(accConf.getUserName());

		// Check if the table exists
		if (!conn.tableOperations().exists(tableName)) {
			System.err.println("Table " + tableName + " does not exist.");
			return 1;
		}

		// Create AccumuloBackedGraph and set view
		AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);
		// 	- Time window
		if (useTimeWindow) {
			graph.setTimeWindow(startDate, endDate);
		}
		//  - Roll up over time and visibility iterator
		graph.rollUpOverTimeAndVisibility(rollUpOverTimeAndVisibility);
		// 	- If not specifying seeds then add iterator to avoid seeing the same edge multiple times
		if (seedsSpecified) {
			Set<TypeValue> typeValues = new HashSet<TypeValue>();
			BufferedReader reader = new BufferedReader(new FileReader(seedsFile));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split("\\|");
				if (tokens.length != 2) {
					System.err.println("Invalid line: " + line);
					continue;
				}
				String type = tokens[0];
				String value = tokens[1];
				typeValues.add(new TypeValue(type, value));
			}
			reader.close();
			// Use AccumuloBackedGraph to update the configuration with the view added above
			graph.setConfiguration(conf, typeValues, accConf);
		} else {
			// Use AccumuloBackedGraph to update the configuration with the view added above
			graph.setConfiguration(conf, accConf);
		}

		// Conf
		conf.setBoolean("mapred.compress.map.output", true);
		conf.setClass("mapred.map.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
		
		// Job
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName("Example MapReduce against Gaffer data in Accumulo format: input = "
				+ tableName + ", output = " + outputPath);

		// Input format - use BatchScannerElementInputFormat if seeds have been specified (as that creates fewer
		// splits); otherwise use ElementInputFormat which is based on the standard AccumuloInputFormat.
		if (seedsSpecified) {
			job.setInputFormatClass(BatchScannerElementInputFormat.class);
		} else {
			job.setInputFormatClass(ElementInputFormat.class);
		}

		// Mapper
		job.setMapperClass(ExampleMapper.class);
		job.setMapOutputKeyClass(GraphElement.class);
		job.setMapOutputValueClass(SetOfStatistics.class);

		// Reducer - use default IdentityReducer for this example
		job.setOutputKeyClass(GraphElement.class);
		job.setOutputValueClass(SetOfStatistics.class);
		job.setNumReduceTasks(numReduceTasks);

		// Output
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		System.out.println("Running MapReduce job over:");
		System.out.println("\tTable: " + accConf.getTable());
		System.out.println("\tUser: " + accConf.getUserName());
		System.out.println("\tAuths: " + authorizations);
		if (useTimeWindow) {
			System.out.println("\tFilter by time: start time is " + DATE_FORMAT.format(startDate)
					+ ", " + DATE_FORMAT.format(endDate));
		} else {
			System.out.println("\tFilter by time is off");
		}
		System.out.println("\tRoll up over time and visibility: "
				+ rollUpOverTimeAndVisibility);
		
		// Run job
		job.waitForCompletion(true);

		// Successful?
		if (!job.isSuccessful()) {
			System.err.println("Error running job");
			return 1;
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ExampleDriver(), args);
		System.exit(exitCode);
	}

}

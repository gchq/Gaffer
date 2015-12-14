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
import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;

import java.util.Collection;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class to prepare data for bulk import to Accumulo. 
 * 
 * The input is a directory of sequence files containing {@link GraphElement},
 * {@link SetOfStatistics} pairs, an output path, and an Accumulo properties file.
 * This file should contain the following fields:
 * 
 * 	accumulo.instance=instance_name
 * 	accumulo.zookeepers=server1:2181,server2:2181,...
 *  accumulo.table=table_name
 *  accumulo.user=your_accumulo_user_name
 *  accumulo.password=your_accumulo_password
 * 
 * The output is a directory containing files suitable for bulk import to
 * Accumulo. This directory is called data_for_accumulo within the output
 * directory you specified. The data can be moved into Accumulo using
 * {@link MoveIntoAccumulo}.
 */
public class BulkImportDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// Usage
		if (args.length < 3) {
			System.err.println("Usage: " + BulkImportDriver.class.getName() + " <inputpath> <output_path> <accumulo_properties_file>");
			return 1;
		}

		// Gets paths
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1] + "/data_for_accumulo/");
		Path splitsFilePath = new Path(args[1] + "/splits_file");
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

		// Get the current splits from the table.
		// (This assumes that we have already created the table using <code>InitialiseTable</code>.)
		Collection<Text> splits = conn.tableOperations().getSplits(tableName);
		int numSplits = splits.size();
		System.out.println("Number of splits in table is " + numSplits);

		// Write current splits to a file (this is needed so that the following MapReduce
		// job can move them to the DistributedCache).
		IngestUtils.createSplitsFile(conn, tableName, fs, splitsFilePath);

		// Run MapReduce to output data suitable for bulk import to Accumulo
		// Conf and job
		conf.setBoolean("mapred.compress.map.output", true);
		conf.setClass("mapred.map.output.compression.codec", SnappyCodec.class, CompressionCodec.class);
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName("Convert data to Accumulo format: input = "
				+ inputPath + ", output = " + outputPath);

		// Input
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, inputPath);

		// Mapper
		job.setMapperClass(BulkImportMapper.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);

		// Partitioner
		job.setPartitionerClass(KeyRangePartitioner.class);
		KeyRangePartitioner.setSplitFile(job, splitsFilePath.toString());

		// Reducer
		job.setReducerClass(BulkImportReducer.class);
		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(Value.class);
		job.setNumReduceTasks(numSplits + 1);

		// Output
		job.setOutputFormatClass(AccumuloFileOutputFormat.class);
		AccumuloFileOutputFormat.setOutputPath(job, outputPath);

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
		int exitCode = ToolRunner.run(new BulkImportDriver(), args);
		System.exit(exitCode);
	}

}

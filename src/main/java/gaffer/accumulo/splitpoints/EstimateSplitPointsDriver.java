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
package gaffer.accumulo.splitpoints;

import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;

import java.io.BufferedOutputStream;
import java.io.PrintStream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A driver class to estimate the split points to ensure an even distribution of data
 * in Accumulo for the initial split.
 * 
 * The data to be used as input is {@link SequenceFile}s of
 * {@link GraphElement}, {@link SetOfStatistics} pairs. This data is randomly sampled
 * (with a probability of sampling specified by the user) and sent to one reducer.
 * Because it is sent to one reducer, the sampling probability should be set low
 * enough that one reducer can handle that amount of data. A sampling probability
 * of 0.01 is often used.
 * 
 * Once the MapReduce job has finished, the single results file is read through
 * in order and split points are output to a file.
 */
public class EstimateSplitPointsDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length < 5) {
			System.err.println("Usage: " + this.getClass().getName()
					+ " <mapred_output_directory> <proportion_to_sample> <number_of_tablet_servers> <resulting_split_file> <input_path1>...");
			return 1;
		}
		
		// Parse arguments
		Path outputPath = new Path(args[0]);
		float proportionToSample = Float.parseFloat(args[1]);
		int numberTabletServers = Integer.parseInt(args[2]);
		Path resultingSplitsFile = new Path(args[3]);
		Path[] inputPaths = new Path[args.length - 4];
		for (int i = 0; i < inputPaths.length; i++) {
			inputPaths[i] = new Path(args[i + 4]);
		}
		
		// Conf and job
		Configuration conf = getConf();
		conf.setFloat("proportion_to_sample", proportionToSample);
		String jobName = "Estimate split points: input = ";
		for (int i = 0; i < inputPaths.length; i++) {
			jobName += inputPaths[i]+ ", ";
		}
		jobName += "output = " + outputPath;
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(getClass());

		// Input
		job.setInputFormatClass(SequenceFileInputFormat.class);
		for (int i = 0; i < inputPaths.length; i++) {
			SequenceFileInputFormat.addInputPath(job, inputPaths[i]);
		}

		// Mapper
		job.setMapperClass(EstimateSplitPointsMapper.class);
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		
		// Reducer
		job.setReducerClass(EstimateSplitPointsReducer.class);
		job.setOutputKeyClass(Key.class);
		job.setOutputValueClass(Value.class);
		job.setNumReduceTasks(1);

		// Output
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, outputPath);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		// Run job
		job.waitForCompletion(true);

		// Successful?
		if (!job.isSuccessful()) {
			System.err.println("Error running job");
			return 1;
		}
		
		// Number of records output
		// NB In the following line use mapred.Task.Counter.REDUCE_OUTPUT_RECORDS rather than
		// mapreduce.TaskCounter.REDUCE_OUTPUT_RECORDS as this is more compatible with earlier
		// versions of Hadoop.
		@SuppressWarnings("deprecation")
		Counter counter = job.getCounters().findCounter(org.apache.hadoop.mapred.Task.Counter.REDUCE_OUTPUT_RECORDS);
		long recordsOutput = counter.getValue();
		System.out.println("Number of records output = " + recordsOutput);
		
		// Work out when to output a split point. The number of split points
		// needed is the number of tablet servers minus 1 (because you don't
		// have to output the start of the first tablet or the end of the
		// last tablet).
		long outputEveryNthRecord = recordsOutput / (numberTabletServers - 1);
		
		// Read through resulting file, pick out the split points and write to
		// file.
		FileSystem fs = FileSystem.get(conf);
		Path resultsFile = new Path(outputPath, "part-r-00000");
		@SuppressWarnings("deprecation")
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, resultsFile, conf);
		PrintStream splitsWriter = new PrintStream(new BufferedOutputStream(fs.create(resultingSplitsFile, true)));
		Key key = new Key();
		Value value = new Value();
		long count = 0;
		int numberSplitPointsOutput = 0;
		while (reader.next(key, value) && numberSplitPointsOutput < numberTabletServers - 1) {
			count++;
			if (count % outputEveryNthRecord == 0) {
				numberSplitPointsOutput++;
				splitsWriter.println(new String(Base64.encodeBase64(key.getRow().getBytes())));
				System.out.println("Written split point: " + key.getRow());
			}
		}
		reader.close();
		splitsWriter.close();
		System.out.println("Number of split points output = " + numberSplitPointsOutput);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new EstimateSplitPointsDriver(), args);
		System.exit(exitCode);
	}

}

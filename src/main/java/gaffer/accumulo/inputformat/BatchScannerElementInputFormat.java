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
package gaffer.accumulo.inputformat;

import geotrellis.spark.io.accumulo.BatchAccumuloInputFormat;
import geotrellis.spark.io.accumulo.MultiRangeInputSplit;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.utils.WritableToStringConverter;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An {@link InputFormat} that allows a MapReduce job to consume data from the
 * Accumulo table underlying Gaffer. This uses {@link BatchScannerElementInputFormat} to provide the
 * splits and uses a {@link BatchScanner} within the {@link RecordReader} to provide the keys
 * and values.
 */
public class BatchScannerElementInputFormat extends InputFormatBase<GraphElement, SetOfStatistics> {

	public final static String POST_ROLL_UP_TRANSFORM = "POST_ROLL_UP_TRANSFORM";

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		return new BatchAccumuloInputFormat().getSplits(context);
	}

	@Override
	public RecordReader<GraphElement, SetOfStatistics> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		log.setLevel(getLogLevel(context));
		String serialisedPostRollUpTransform = context.getConfiguration().get(POST_ROLL_UP_TRANSFORM);
		if (serialisedPostRollUpTransform != null) {
			try {
				Transform transform = (Transform) WritableToStringConverter.deserialiseFromString(serialisedPostRollUpTransform);
				return new BatchScannerRecordReader(transform);
			} catch (IOException e) {
				throw new IllegalArgumentException("Unable to deserialise a Transform from the string: " + serialisedPostRollUpTransform);
			} catch (ClassCastException e) {
				throw new IllegalArgumentException("Unable to deserialise a Transform from the string: " + serialisedPostRollUpTransform);
			}
		}
		return new BatchScannerRecordReader();
	}

	class BatchScannerRecordReader extends RecordReader<GraphElement, SetOfStatistics> {

		private boolean postRollUpTransformSpecified;
		private Transform transform;
		private BatchScanner scanner;
		private Iterator<Map.Entry<Key, Value>> scannerIterator;
		private GraphElement currentK;
		private SetOfStatistics currentV;

		public BatchScannerRecordReader() throws IOException, InterruptedException {
			super();
			this.postRollUpTransformSpecified = false;
		}

		public BatchScannerRecordReader(Transform transform) throws IOException, InterruptedException {
			this();
			this.postRollUpTransformSpecified = true;
			this.transform = transform;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			MultiRangeInputSplit inputSplit = (MultiRangeInputSplit) split;
			Connector connector = inputSplit.connector();
			try {
				Authorizations auths = InputConfigurator.getScanAuthorizations(AccumuloInputFormat.class, context.getConfiguration());
				log.info("Initialising BatchScanner on table " + inputSplit.table() + " with auths " + auths);
				scanner = connector.createBatchScanner(inputSplit.table(), auths, 1);
			} catch (TableNotFoundException e) {
				throw new IOException("Exception whilst initializing batch scanner: " + e);
			}
			scanner.setRanges(JavaConverters.asJavaCollectionConverter(inputSplit.ranges()).asJavaCollection());
			List<IteratorSetting> iteratorSettings = JavaConverters.asJavaListConverter(inputSplit.iterators()).asJava();
			for (IteratorSetting is : iteratorSettings) {
				log.debug("Adding scan iterator " + is);
				scanner.addScanIterator(is);
			}
			Collection<Pair<Text, Text>> fetchedColumns = JavaConverters.asJavaCollectionConverter(inputSplit.fetchedColumns()).asJavaCollection();
			for (Pair<Text, Text> pair : fetchedColumns) {
				if (pair.getSecond() != null) {
					scanner.fetchColumn(pair.getFirst(), pair.getSecond());
				} else {
					scanner.fetchColumnFamily(pair.getFirst());
				}
			}
			scannerIterator = scanner.iterator();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (scannerIterator.hasNext()) {
				Map.Entry<Key,Value> entry = scannerIterator.next();
				currentK = ConversionUtils.getGraphElementFromKey(entry.getKey());
				currentV = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
				// Transform if a transform has been specified
				if (postRollUpTransformSpecified) {
					GraphElementWithStatistics transformed = transform.transform(new GraphElementWithStatistics(currentK, currentV));
					currentK = transformed.getGraphElement();
					currentV = transformed.getSetOfStatistics();
				}
				return true;
			}
			return false;
		}

		@Override
		public GraphElement getCurrentKey() throws IOException, InterruptedException {
			return currentK;
		}

		@Override
		public SetOfStatistics getCurrentValue() throws IOException, InterruptedException {
			return currentV;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0.0F;
		}

		@Override
		public void close() throws IOException {
			scanner.close();
		}
	}

}

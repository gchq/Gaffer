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

import gaffer.accumulo.ConversionUtils;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;

import java.io.IOException;
import java.util.Map.Entry;

import gaffer.utils.WritableToStringConverter;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An {@link InputFormat} that allows a MapReduce job to consume data from the
 * Accumulo table underlying Gaffer.
 */
public class ElementInputFormat extends InputFormatBase<GraphElement, SetOfStatistics> {

	public final static String POST_ROLL_UP_TRANSFORM = "POST_ROLL_UP_TRANSFORM";

	@Override
	public RecordReader<GraphElement, SetOfStatistics> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		log.setLevel(getLogLevel(context));
		String serialisedPostRollUpTransform = context.getConfiguration().get(POST_ROLL_UP_TRANSFORM);
		if (serialisedPostRollUpTransform != null) {
			try {
				Transform transform = (Transform) WritableToStringConverter.deserialiseFromString(serialisedPostRollUpTransform);
				return new ElementWithStatisticsRecordReader(transform);
			} catch (IOException e) {
				throw new IllegalArgumentException("Unable to deserialise a Transform from the string: " + serialisedPostRollUpTransform);
			} catch (ClassCastException e) {
				throw new IllegalArgumentException("Unable to deserialise a Transform from the string: " + serialisedPostRollUpTransform);
			}
		}
		return new ElementWithStatisticsRecordReader();
	}

	class ElementWithStatisticsRecordReader extends RecordReaderBase<GraphElement, SetOfStatistics> {

		private boolean postRollUpTransformSpecified;
		private Transform transform;

		public ElementWithStatisticsRecordReader() {
			super();
			this.postRollUpTransformSpecified = false;
		}

		public ElementWithStatisticsRecordReader(Transform transform) {
			this();
			this.postRollUpTransformSpecified = true;
			this.transform = transform;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (scannerIterator.hasNext()) {
				++numKeysRead;
				Entry<Key, Value> entry = scannerIterator.next();
				currentK = ConversionUtils.getGraphElementFromKey(entry.getKey());
				currentV = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
				if (log.isTraceEnabled()) {
					log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
				}
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
	}

}

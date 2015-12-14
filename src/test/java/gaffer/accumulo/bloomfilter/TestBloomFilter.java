/**
 * Copyright 2015 Crown Copyright
 * Copyright 2011-2015 The Apache Software Foundation
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
package gaffer.accumulo.bloomfilter;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import gaffer.Pair;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.SimpleTimeZone;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.codehaus.plexus.util.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the performance of the Bloom filter - checks that looking up random data is quicker
 * than looking up data that is present.
 *
 * This class is based on Accumulo's BloomFilterLayerLookupTest (org.apache.accumulo.core.file.BloomFilterLayerLookupTest).
 */
public class TestBloomFilter {

	private final static Logger LOGGER = LoggerFactory.getLogger(TestBloomFilter.class);
	private final static String VISIBILITY = "public";
	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void test() {
		try {
			// Create random data to insert, and sort it
			Random random = new Random();
			HashSet<Key> keysSet = new HashSet<Key>();
			HashSet<TypeValue> dataSet = new HashSet<TypeValue>();
			Date startDate = DATE_FORMAT.parse("20140101000000");
			Date endDate = DATE_FORMAT.parse("20140101000000");
			for (int i = 0; i < 100000; i++) {
				TypeValue source = new TypeValue("type", "" + random.nextInt(Integer.MAX_VALUE));
				TypeValue destination = new TypeValue("type", "" + random.nextInt(Integer.MAX_VALUE));
				dataSet.add(source);
				dataSet.add(destination);
				Entity sourceEntity = new Entity(source.getType(), source.getValue(), "info", "subinfo", VISIBILITY, startDate, endDate);
				Entity destinationEntity = new Entity(destination.getType(), destination.getValue(), "info", "subinfo", VISIBILITY, startDate, endDate);
				Edge edge = new Edge("type", "" + source, "type", "" + destination, "type", "subtype", true, VISIBILITY, startDate, endDate);
				keysSet.add(ConversionUtils.getKeyFromEntity(sourceEntity));
				keysSet.add(ConversionUtils.getKeyFromEntity(destinationEntity));
				Pair<Key> edgeKeys = ConversionUtils.getKeysFromEdge(edge);
				keysSet.add(edgeKeys.getFirst());
				keysSet.add(edgeKeys.getSecond());
			}
			ArrayList<Key> keys = new ArrayList<Key>(keysSet);
			Collections.sort(keys);
			SetOfStatistics stats = new SetOfStatistics();
			stats.addStatistic("count", new Count(10));
			Value value = ConversionUtils.getValueFromSetOfStatistics(stats);

			// Create Accumulo configuration
			ConfigurationCopy accumuloConf = new ConfigurationCopy(AccumuloConfiguration.getDefaultConfiguration());
			accumuloConf.set(Property.TABLE_BLOOM_ENABLED, "true");
			accumuloConf.set(Property.TABLE_BLOOM_KEY_FUNCTOR, ElementFunctor.class.getName());
			accumuloConf.set(Property.TABLE_FILE_TYPE, RFile.EXTENSION);
			accumuloConf.set(Property.TABLE_BLOOM_LOAD_THRESHOLD, "1");
			accumuloConf.set(Property.TSERV_BLOOM_LOAD_MAXCONCURRENT, "1");

			// Create Hadoop configuration
			Configuration conf = CachedConfiguration.getInstance();
			FileSystem fs = FileSystem.get(conf);

			// Open file
			String suffix = FileOperations.getNewFileExtension(accumuloConf);
			String filenameTemp = tempFolder.newFile().getAbsolutePath();
			FileUtils.fileDelete(filenameTemp);
			String filename = filenameTemp + "." + suffix;
			File file = new File(filename);
			if (file.exists()) {
				file.delete();
			}
			FileSKVWriter writer = FileOperations.getInstance().openWriter(filename, fs, conf, accumuloConf);

			// Write data to file
			writer.startDefaultLocalityGroup();
			for (Key key : keys) {
				writer.append(key, value);
			}
			writer.close();

			// Reader
			FileSKVIterator reader = FileOperations.getInstance().openReader(filename, false, fs, conf, accumuloConf);
			
			// Calculate random look up rate - run it 3 times and take best
			int numTrials = 5;
			double maxRandomRate = -1.0;
			for (int i = 0; i < numTrials; i++) {
				double rate = calculateRandomLookUpRate(reader, dataSet, random);
				if (rate > maxRandomRate) {
					maxRandomRate = rate;
				}
			}
			LOGGER.info("Max random rate = {}", maxRandomRate);
			
			// Calculate look up rate for items that were inserted
			double maxCausalRate = -1.0;
			for (int i = 0; i < numTrials; i++) {
				double rate = calculateCausalLookUpRate(reader, dataSet, random);
				if (rate > maxCausalRate) {
					maxCausalRate = rate;
				}
			}
			LOGGER.info("Max causal rate = {}", maxCausalRate);
			
			// Random look up rate should be much faster
			assertTrue(maxRandomRate > maxCausalRate);

			// Close reader
			reader.close();
		} catch (IOException e) {
			fail("IOException " + e);
		} catch (ParseException e) {
			fail("ParseException " + e);
		}
	}

	private double calculateRandomLookUpRate(FileSKVIterator reader, HashSet<TypeValue> dataSet, Random random) throws IOException {
		TypeValue[] randomData = new TypeValue[5000];
		for (int i = 0; i < 5000; i++) {
			randomData[i] = new TypeValue("type", "" + random.nextInt(Integer.MAX_VALUE));
		}
		long start = System.currentTimeMillis();
		for (int i = 0; i < 5000; i++) {
			seek(reader, randomData[i].getType(), randomData[i].getValue());
			if (dataSet.contains(randomData[i])) {
				assertTrue(reader.hasTop());
			}
		}
		long end = System.currentTimeMillis();
		double randomRate = 5000 / ((end - start) / 1000.0);
		LOGGER.info("Random look up rate = {}", randomRate);
		return randomRate;
	}
	
	private double calculateCausalLookUpRate(FileSKVIterator reader, HashSet<TypeValue> dataSet, Random random) throws IOException {
		int count = 0;
		long start = System.currentTimeMillis();
		for (TypeValue typeValue : dataSet) {
			seek(reader, typeValue.getType(), typeValue.getValue());
			assertTrue(reader.hasTop());
			count++;
			if (count >= 5000) {
				break;
			}
		}
		long end = System.currentTimeMillis();
		double causalRate = 5000 / ((end - start) / 1000.0);
		LOGGER.info("Causal look up rate = {}", causalRate);
		return causalRate;
	}
	
	private void seek(FileSKVIterator reader, String type, String value) throws IOException {
		Range range = ConversionUtils.getRangeFromTypeAndValue(type, value);
		reader.seek(range, new ArrayList<ByteSequence>(), false);
	}
}

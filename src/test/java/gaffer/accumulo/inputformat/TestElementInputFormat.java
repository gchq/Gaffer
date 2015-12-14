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

import static org.junit.Assert.assertEquals;

import gaffer.Pair;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.TableUtils;
import gaffer.accumulo.utils.Accumulo;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;
import gaffer.graph.TypeValueRange;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.*;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.codehaus.plexus.util.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link ElementInputFormat} and {@link BatchScannerElementInputFormat}. Contains tests that
 * the same edges are not processed multiple times when a full table scan is performed, as well as testing
 * that the view is applied correctly.
 *
 * Note that these tests fail unless Accumulo's guava dependency is excluded (see comments in the pom.xml).
 */
public class TestElementInputFormat {

	private enum InputFormatType {ELEMENT_INPUT_FORMAT, BATCH_SCANNER_ELEMENT_INPUT_FORMAT}

	private static Date sevenDaysBefore;
	private static Date sixDaysBefore;
	private static Date fiveDaysBefore;
	private static String visibilityString1 = "private";
	private static String visibilityString2 = "public";

	static {
		Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
		sevenDaysBeforeCalendar.setTime(new Date(System.currentTimeMillis()));
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
		sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
		sixDaysBefore = sevenDaysBeforeCalendar.getTime();
		sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
		fiveDaysBefore = sevenDaysBeforeCalendar.getTime();
	}

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testOnlyGetEdgeOnce() throws Exception {
		testOnlyGetEdgeOnce(InputFormatType.ELEMENT_INPUT_FORMAT);
		testOnlyGetEdgeOnce(InputFormatType.BATCH_SCANNER_ELEMENT_INPUT_FORMAT);
	}

	public void testOnlyGetEdgeOnce(InputFormatType type) throws Exception {
		String INSTANCE_NAME = "A";
		String tableName = "testOnlyGetEdgeOnce" + type.name();
		MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
		Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations(visibilityString1, visibilityString2));
		TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
		Set<GraphElementWithStatistics> data = getData();
		AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
		graph.setAuthorizations(new Authorizations(visibilityString1, visibilityString2));
		graph.addGraphElementsWithStatistics(data);

		// Set up local conf
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		FileSystem fs = FileSystem.getLocal(conf);
		Driver driver = new Driver(type);
		driver.setConf(conf);

		// Create output folder for MapReduce job
		String outputDir = tempFolder.newFolder().getAbsolutePath();
		FileUtils.deleteDirectory(outputDir);

		// Write properties file
		String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
		BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
		bw.write("accumulo.instance=" + INSTANCE_NAME + "\n");
		bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
		bw.write("accumulo.table=" + tableName + "\n");
		bw.write("accumulo.user=root\n");
		bw.write("accumulo.password=\n");
		bw.close();

		// Run job
		assertEquals(0, driver.run(new String[]{accumuloPropertiesFilename, outputDir}));

		// Read results in
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputDir + "/part-m-00000"), conf);
		GraphElement element = new GraphElement();
		SetOfStatistics statistics = new SetOfStatistics();
		int count = 0;
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		while (reader.next(element, statistics)) {
			count++;
			results.add(new GraphElementWithStatistics(element.clone(), statistics.clone()));
		}
		reader.close();
		// There should be 3 edges and 2 entities - 5 in total
		// Note need to check count (and not just compare sets) as the point of the test is to
		// ensure we don't get the same edge back twice
		assertEquals(5, count);
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("count", new Count(20));
		statistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
		Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics2 = new SetOfStatistics();
		statistics2.addStatistic("countSomething", new Count(123456));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));
		Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		statistics5.addStatistic("count", new Count(99));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		statisticsEntity2.addStatistic("entity_count", new Count(12345));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

		assertEquals(results, expectedResults);
	}

	@Test
	public void testAuthsAreEnforced() throws Exception {
		testAuthsAreEnforced(InputFormatType.ELEMENT_INPUT_FORMAT);
		testAuthsAreEnforced(InputFormatType.BATCH_SCANNER_ELEMENT_INPUT_FORMAT);
	}

	public void testAuthsAreEnforced(InputFormatType type) throws Exception {
		String INSTANCE_NAME = "A";
		String tableName = "testAuthsAreEnforced" + type.name();
		MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
		Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
		// Only give them permission to see visibilityString1
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations(visibilityString1));
		TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
		Set<GraphElementWithStatistics> data = getData();
		AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
		graph.addGraphElementsWithStatistics(data);

		// Set up local conf
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		FileSystem fs = FileSystem.getLocal(conf);
		Driver driver = new Driver(type);
		driver.setConf(conf);

		// Create output folder for MapReduce job
		String outputDir = tempFolder.newFolder().getAbsolutePath();
		FileUtils.deleteDirectory(outputDir);

		// Write properties file
		String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
		BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
		bw.write("accumulo.instance=" + INSTANCE_NAME + "\n");
		bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
		bw.write("accumulo.table=" + tableName + "\n");
		bw.write("accumulo.user=root\n");
		bw.write("accumulo.password=\n");
		bw.close();

		// Run job
		assertEquals(0, driver.run(new String[]{accumuloPropertiesFilename, outputDir}));

		// Read results in
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputDir + "/part-m-00000"), conf);
		GraphElement element = new GraphElement();
		SetOfStatistics statistics = new SetOfStatistics();
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		int count = 0;
		while (reader.next(element, statistics)) {
			results.add(new GraphElementWithStatistics(element.clone(), statistics.clone()));
			count++;
		}
		reader.close();

		// There should be 1 edge and 2 entities - 3 in total
		assertEquals(3, count);
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("count", new Count(3));
		statistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		statisticsEntity2.addStatistic("entity_count", new Count(12345));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

		assertEquals(results, expectedResults);
	}

	@Test
	public void testAuthsCanBeRestrictedByUser() throws Exception {
		testAuthsCanBeRestrictedByUser(InputFormatType.ELEMENT_INPUT_FORMAT);
		testAuthsCanBeRestrictedByUser(InputFormatType.BATCH_SCANNER_ELEMENT_INPUT_FORMAT);
	}

	public void testAuthsCanBeRestrictedByUser(InputFormatType type) throws Exception {
		String INSTANCE_NAME = "A";
		String tableName = "testAuthsCanBeRestrictedByUser" + type.name();
		MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
		Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
		// Give them permission to see visibilityString1 and 2
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations(visibilityString1, visibilityString2));
		TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
		Set<GraphElementWithStatistics> data = getData();
		AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
		graph.addGraphElementsWithStatistics(data);

		// Set up local conf
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		FileSystem fs = FileSystem.getLocal(conf);
		// Choose to only see data with visibilityString1
		DriverRestrictedAuths driver = new DriverRestrictedAuths(type, new Authorizations(visibilityString1));
		driver.setConf(conf);

		// Create output folder for MapReduce job
		String outputDir = tempFolder.newFolder().getAbsolutePath();
		FileUtils.deleteDirectory(outputDir);

		// Write properties file
		String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
		BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
		bw.write("accumulo.instance=" + INSTANCE_NAME + "\n");
		bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
		bw.write("accumulo.table=" + tableName + "\n");
		bw.write("accumulo.user=root\n");
		bw.write("accumulo.password=\n");
		bw.close();

		// Run job
		assertEquals(0, driver.run(new String[]{accumuloPropertiesFilename, outputDir}));

		// Read results in
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputDir + "/part-m-00000"), conf);
		GraphElement element = new GraphElement();
		SetOfStatistics statistics = new SetOfStatistics();
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		int count = 0;
		while (reader.next(element, statistics)) {
			results.add(new GraphElementWithStatistics(element.clone(), statistics.clone()));
			count++;
		}
		reader.close();

		// There should be 1 edge and 2 entities - 3 in total
		assertEquals(3, count);
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("count", new Count(3));
		statistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		statisticsEntity2.addStatistic("entity_count", new Count(12345));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

		assertEquals(results, expectedResults);
	}

	@Test
	public void testPostRollUpTransformIsAppliedReadingWholeTable() throws Exception {
		testPostRollUpTransformIsAppliedReadingWholeTable(InputFormatType.ELEMENT_INPUT_FORMAT);
		testPostRollUpTransformIsAppliedReadingWholeTable(InputFormatType.BATCH_SCANNER_ELEMENT_INPUT_FORMAT);
	}

	public void testPostRollUpTransformIsAppliedReadingWholeTable(InputFormatType type) throws Exception {
		String INSTANCE_NAME = "A";
		String tableName = "testPostRollUpTransformIsAppliedReadingWholeTable" + type.name();
		MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
		Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations(visibilityString1, visibilityString2));
		TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
		Set<GraphElementWithStatistics> data = getData();
		AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
		graph.addGraphElementsWithStatistics(data);

		// Add post roll-up transform
		String transformedSummaryType = "abc";

		// Set up local conf
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		FileSystem fs = FileSystem.getLocal(conf);
		Driver driver = new Driver(type, new ExampleTransform(transformedSummaryType));
		driver.setConf(conf);

		// Create output folder for MapReduce job
		String outputDir = tempFolder.newFolder().getAbsolutePath();
		FileUtils.deleteDirectory(outputDir);

		// Write properties file
		String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
		BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
		bw.write("accumulo.instance=" + INSTANCE_NAME + "\n");
		bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
		bw.write("accumulo.table=" + tableName + "\n");
		bw.write("accumulo.user=root\n");
		bw.write("accumulo.password=\n");
		bw.close();

		// Run job
		assertEquals(0, driver.run(new String[]{accumuloPropertiesFilename, outputDir}));

		// Read results in
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputDir + "/part-m-00000"), conf);
		GraphElement element = new GraphElement();
		SetOfStatistics statistics = new SetOfStatistics();
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		while (reader.next(element, statistics)) {
			results.add(new GraphElementWithStatistics(element.clone(), statistics.clone()));
			assertEquals(transformedSummaryType, element.getSummaryType());
		}
		reader.close();

		// There should be 3 edges and 2 entities - 5 in total
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge1 = new Edge("customer", "A", "product", "P", transformedSummaryType, "instore", true, visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("count", new Count(20));
		statistics1.addStatistic("anotherCount", new Count(1000000));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
		Edge edge2 = new Edge("customer", "A", "product", "P", transformedSummaryType, "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics2 = new SetOfStatistics();
		statistics2.addStatistic("countSomething", new Count(123456));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));
		Edge edge5 = new Edge("customer", "B", "product", "Q", transformedSummaryType, "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		statistics5.addStatistic("count", new Count(99));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));
		Entity entity1 = new Entity("customer", "A", transformedSummaryType, "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
		Entity entity2 = new Entity("product", "R", transformedSummaryType, "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		statisticsEntity2.addStatistic("entity_count", new Count(12345));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

		assertEquals(expectedResults, results);
	}

	@Test
	public void testPostRollUpTransformIsAppliedReadingDataForOneEntity() throws Exception {
		testPostRollUpTransformIsAppliedReadingDataForOneEntity(InputFormatType.ELEMENT_INPUT_FORMAT);
		testPostRollUpTransformIsAppliedReadingDataForOneEntity(InputFormatType.BATCH_SCANNER_ELEMENT_INPUT_FORMAT);
	}

	public void testPostRollUpTransformIsAppliedReadingDataForOneEntity(InputFormatType type) throws Exception {
		String INSTANCE_NAME = "A";
		String tableName = "testPostRollUpTransformIsAppliedReadingDataForOneEntity" + type.name();
		MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
		Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations(visibilityString1, visibilityString2));// TEMP _ CHECK
		TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
		Set<GraphElementWithStatistics> data = getData();
		AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
		graph.addGraphElementsWithStatistics(data);

		// Add post roll-up transform
		String transformedSummaryType = "abc";

		// Set up local conf
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		FileSystem fs = FileSystem.getLocal(conf);
		DriverForCertainTypeValues driver = new DriverForCertainTypeValues(type, new ExampleTransform(transformedSummaryType));
		driver.setConf(conf);

		// Create output folder for MapReduce job
		String outputDir = tempFolder.newFolder().getAbsolutePath();
		FileUtils.deleteDirectory(outputDir);

		// Write properties file
		String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
		BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
		bw.write("accumulo.instance=" + INSTANCE_NAME + "\n");
		bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
		bw.write("accumulo.table=" + tableName + "\n");
		bw.write("accumulo.user=root\n");
		bw.write("accumulo.password=\n");
		bw.close();

		// Run job
		assertEquals(0, driver.run(new String[]{accumuloPropertiesFilename, outputDir, "customer", "B"}));

		// Read results in
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputDir + "/part-m-00000"), conf);
		GraphElement element = new GraphElement();
		SetOfStatistics statistics = new SetOfStatistics();
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		while (reader.next(element, statistics)) {
			results.add(new GraphElementWithStatistics(element.clone(), statistics.clone()));
			assertEquals(transformedSummaryType, element.getSummaryType());
		}
		reader.close();

		// There should be 1 edge
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", transformedSummaryType, "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		statistics5.addStatistic("count", new Count(99));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

		assertEquals(expectedResults, results);
	}

	@Test
	public void testPostRollUpTransformIsAppliedReadingDataForOneRange() throws Exception {
		testPostRollUpTransformIsAppliedReadingDataForOneRange(InputFormatType.ELEMENT_INPUT_FORMAT);
		testPostRollUpTransformIsAppliedReadingDataForOneRange(InputFormatType.BATCH_SCANNER_ELEMENT_INPUT_FORMAT);
	}

	public void testPostRollUpTransformIsAppliedReadingDataForOneRange(InputFormatType type) throws Exception {
		String INSTANCE_NAME = "A";
		String tableName = "testPostRollUpTransformIsAppliedReadingDataForOneRange" + type.name();
		MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
		Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations(visibilityString1, visibilityString2));
		TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
		Set<GraphElementWithStatistics> data = getData();
		AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
		graph.addGraphElementsWithStatistics(data);

		// Add post roll-up transform
		String transformedSummaryType = "abc";

		// Set up local conf
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		FileSystem fs = FileSystem.getLocal(conf);
		DriverForRanges driver = new DriverForRanges(type, new ExampleTransform(transformedSummaryType));
		driver.setConf(conf);

		// Create output folder for MapReduce job
		String outputDir = tempFolder.newFolder().getAbsolutePath();
		FileUtils.deleteDirectory(outputDir);

		// Write properties file
		String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
		BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
		bw.write("accumulo.instance=" + INSTANCE_NAME + "\n");
		bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
		bw.write("accumulo.table=" + tableName + "\n");
		bw.write("accumulo.user=root\n");
		bw.write("accumulo.password=\n");
		bw.close();

		// Run job
		assertEquals(0, driver.run(new String[]{accumuloPropertiesFilename, outputDir, "customer", "B", "customer", "B2"}));

		// Read results in
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputDir + "/part-m-00000"), conf);
		GraphElement element = new GraphElement();
		SetOfStatistics statistics = new SetOfStatistics();
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		while (reader.next(element, statistics)) {
			results.add(new GraphElementWithStatistics(element.clone(), statistics.clone()));
			assertEquals(transformedSummaryType, element.getSummaryType());
		}
		reader.close();

		// There should be 1 edge
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", transformedSummaryType, "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		statistics5.addStatistic("count", new Count(99));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

		assertEquals(results, expectedResults);
	}

	@Test
	public void testPostRollUpTransformIsAppliedReadingDataForPairs() throws Exception {
		testPostRollUpTransformIsAppliedReadingDataForPairs(InputFormatType.ELEMENT_INPUT_FORMAT);
		testPostRollUpTransformIsAppliedReadingDataForPairs(InputFormatType.BATCH_SCANNER_ELEMENT_INPUT_FORMAT);
	}

	public void testPostRollUpTransformIsAppliedReadingDataForPairs(InputFormatType type) throws Exception {
		String INSTANCE_NAME = "A";
		String tableName = "testPostRollUpTransformIsAppliedReadingDataForPairs" + type.name();
		MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
		Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
		connector.securityOperations().changeUserAuthorizations("root", new Authorizations(visibilityString1, visibilityString2));// TEMP _ CHECK
		TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
		Set<GraphElementWithStatistics> data = getData();
		AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
		graph.addGraphElementsWithStatistics(data);

		// Add post roll-up transform
		String transformedSummaryType = "abc";

		// Set up local conf
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		FileSystem fs = FileSystem.getLocal(conf);
		DriverForPairs driver = new DriverForPairs(type, new ExampleTransform(transformedSummaryType));
		driver.setConf(conf);

		// Create output folder for MapReduce job
		String outputDir = tempFolder.newFolder().getAbsolutePath();
		FileUtils.deleteDirectory(outputDir);

		// Write properties file
		String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
		BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
		bw.write("accumulo.instance=" + INSTANCE_NAME + "\n");
		bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
		bw.write("accumulo.table=" + tableName + "\n");
		bw.write("accumulo.user=root\n");
		bw.write("accumulo.password=\n");
		bw.close();

		// Run job
		assertEquals(0, driver.run(new String[]{accumuloPropertiesFilename, outputDir, "customer", "B", "product", "Q"}));

		// Read results in
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputDir + "/part-m-00000"), conf);
		GraphElement element = new GraphElement();
		SetOfStatistics statistics = new SetOfStatistics();
		Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
		while (reader.next(element, statistics)) {
			results.add(new GraphElementWithStatistics(element.clone(), statistics.clone()));
			assertEquals(transformedSummaryType, element.getSummaryType());
		}
		reader.close();

		// There should be 1 edge
		Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
		Edge edge5 = new Edge("customer", "B", "product", "Q", transformedSummaryType, "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		statistics5.addStatistic("count", new Count(99));
		expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

		assertEquals(results, expectedResults);
	}

	class Driver extends Configured implements Tool {

		private InputFormatType type;
		private Transform transform;

		public Driver(InputFormatType type) {
			this.type = type;
		}

		public Driver(InputFormatType type, Transform transform) {
			this.type = type;
			this.transform = transform;
		}

		@Override
		public int run(String[] args) throws Exception {
			if (args.length != 2) {
				throw new IllegalArgumentException("Usage : " + Driver.class.getName() + " <accumulo_properties_file> <output_directory>");
			}
			String accumuloPropertiesFilename = args[0];
			String outputDir = args[1];

			// Create AccumuloConfig from file
			AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFilename);

			// Connect to Accumulo and create AccumuloBackedGraph
			Connector connector = Accumulo.connect(accConf);
			AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, accConf.getTable());
			if (transform != null) {
				graph.setPostRollUpTransform(transform);
			}
			Configuration conf = getConf();

			// Add current graph view to configuration
			graph.setConfiguration(conf, accConf);
			Job job = new Job(conf);
			job.setJarByClass(this.getClass());
			switch (type) {
				case ELEMENT_INPUT_FORMAT: job.setInputFormatClass(ElementInputFormat.class); break;
				case BATCH_SCANNER_ELEMENT_INPUT_FORMAT: job.setInputFormatClass(BatchScannerElementInputFormat.class);
			}
			job.setMapOutputKeyClass(GraphElement.class);
			job.setMapOutputValueClass(SetOfStatistics.class);
			job.setOutputKeyClass(GraphElement.class);
			job.setOutputValueClass(SetOfStatistics.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);

			return job.isSuccessful() ? 0 : 1;
		}

	}

	class DriverRestrictedAuths extends Configured implements Tool {

		private InputFormatType type;
		private Authorizations auths;

		public DriverRestrictedAuths(InputFormatType type) {
			this.type = type;
		}

		public DriverRestrictedAuths(InputFormatType type, Authorizations auths) {
			this.type = type;
			this.auths = auths;
		}

		@Override
		public int run(String[] args) throws Exception {
			if (args.length != 2) {
				throw new IllegalArgumentException("Usage : " + Driver.class.getName() + " <accumulo_properties_file> <output_directory>");
			}
			String accumuloPropertiesFilename = args[0];
			String outputDir = args[1];

			// Create AccumuloConfig from file
			AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFilename);

			// Connect to Accumulo and create AccumuloBackedGraph
			Connector connector = Accumulo.connect(accConf);
			AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, accConf.getTable());
			if (auths != null) {
				graph.setAuthorizations(auths);
			}
			Configuration conf = getConf();

			// Add current graph view to configuration
			graph.setConfiguration(conf, accConf);
			Job job = new Job(conf);
			job.setJarByClass(this.getClass());
			switch (type) {
				case ELEMENT_INPUT_FORMAT: job.setInputFormatClass(ElementInputFormat.class); break;
				case BATCH_SCANNER_ELEMENT_INPUT_FORMAT: job.setInputFormatClass(BatchScannerElementInputFormat.class);
			}
			job.setMapOutputKeyClass(GraphElement.class);
			job.setMapOutputValueClass(SetOfStatistics.class);
			job.setOutputKeyClass(GraphElement.class);
			job.setOutputValueClass(SetOfStatistics.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);

			return job.isSuccessful() ? 0 : 1;
		}

	}

	class DriverForCertainTypeValues extends Configured implements Tool {

		private InputFormatType type;
		private Transform transform;

		public DriverForCertainTypeValues(InputFormatType type, Transform transform) {
			this.type = type;
			this.transform = transform;
		}

		@Override
		public int run(String[] args) throws Exception {
			if (args.length != 4) {
				throw new IllegalArgumentException("Usage : " + Driver.class.getName() + " <accumulo_properties_file> <output_directory> <type> <value>");
			}
			String accumuloPropertiesFilename = args[0];
			String outputDir = args[1];
			TypeValue entity = new TypeValue(args[2], args[3]);

			// Create AccumuloConfig from file
			AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFilename);

			// Connect to Accumulo and create AccumuloBackedGraph
			Connector connector = Accumulo.connect(accConf);
			AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, accConf.getTable());
			if (transform != null) {
				graph.setPostRollUpTransform(transform);
			}
			Configuration conf = getConf();

			// Add current graph view to configuration
			graph.setConfiguration(conf, entity, accConf);
			Job job = new Job(conf);
			job.setJarByClass(this.getClass());
			switch (type) {
				case ELEMENT_INPUT_FORMAT: job.setInputFormatClass(ElementInputFormat.class); break;
				case BATCH_SCANNER_ELEMENT_INPUT_FORMAT: job.setInputFormatClass(BatchScannerElementInputFormat.class);
			}
			job.setMapOutputKeyClass(GraphElement.class);
			job.setMapOutputValueClass(SetOfStatistics.class);
			job.setOutputKeyClass(GraphElement.class);
			job.setOutputValueClass(SetOfStatistics.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);

			return job.isSuccessful() ? 0 : 1;
		}

	}

	class DriverForRanges extends Configured implements Tool {

		private InputFormatType type;
		private Transform transform;

		public DriverForRanges(InputFormatType type, Transform transform) {
			this.type = type;
			this.transform = transform;
		}

		@Override
		public int run(String[] args) throws Exception {
			if (args.length != 6) {
				throw new IllegalArgumentException("Usage : " + Driver.class.getName() + " <accumulo_properties_file> <output_directory> <starttype> <startvalue> <endtype> <endvalue>");
			}
			String accumuloPropertiesFilename = args[0];
			String outputDir = args[1];
			TypeValueRange range = new TypeValueRange(args[2], args[3], args[4], args[5]);

			// Create AccumuloConfig from file
			AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFilename);

			// Connect to Accumulo and create AccumuloBackedGraph
			Connector connector = Accumulo.connect(accConf);
			AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, accConf.getTable());
			if (transform != null) {
				graph.setPostRollUpTransform(transform);
			}
			Configuration conf = getConf();

			// Add current graph view to configuration
			graph.setConfigurationFromRanges(conf, range, accConf);
			Job job = new Job(conf);
			job.setJarByClass(this.getClass());
			switch (type) {
				case ELEMENT_INPUT_FORMAT: job.setInputFormatClass(ElementInputFormat.class); break;
				case BATCH_SCANNER_ELEMENT_INPUT_FORMAT: job.setInputFormatClass(BatchScannerElementInputFormat.class);
			}
			job.setMapOutputKeyClass(GraphElement.class);
			job.setMapOutputValueClass(SetOfStatistics.class);
			job.setOutputKeyClass(GraphElement.class);
			job.setOutputValueClass(SetOfStatistics.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);

			return job.isSuccessful() ? 0 : 1;
		}

	}

	class DriverForPairs extends Configured implements Tool {

		private InputFormatType type;
		private Transform transform;

		public DriverForPairs(InputFormatType type, Transform transform) {
			this.type = type;
			this.transform = transform;
		}

		@Override
		public int run(String[] args) throws Exception {
			if (args.length != 6) {
				throw new IllegalArgumentException("Usage : " + Driver.class.getName() + " <accumulo_properties_file> <output_directory> <type1> <value1> <type2> <value2>");
			}
			String accumuloPropertiesFilename = args[0];
			String outputDir = args[1];
			TypeValue first = new TypeValue(args[2], args[3]);
			TypeValue second = new TypeValue(args[4], args[5]);
			Pair<TypeValue> pair = new Pair<TypeValue>(first, second);

			// Create AccumuloConfig from file
			AccumuloConfig accConf = new AccumuloConfig(accumuloPropertiesFilename);

			// Connect to Accumulo and create AccumuloBackedGraph
			Connector connector = Accumulo.connect(accConf);
			AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, accConf.getTable());
			if (transform != null) {
				graph.setPostRollUpTransform(transform);
			}
			Configuration conf = getConf();

			// Add current graph view to configuration
			graph.setConfigurationFromPairs(conf, pair, accConf);
			Job job = new Job(conf);
			job.setJarByClass(this.getClass());
			switch (type) {
				case ELEMENT_INPUT_FORMAT: job.setInputFormatClass(ElementInputFormat.class); break;
				case BATCH_SCANNER_ELEMENT_INPUT_FORMAT: job.setInputFormatClass(BatchScannerElementInputFormat.class);
			}
			job.setMapOutputKeyClass(GraphElement.class);
			job.setMapOutputValueClass(SetOfStatistics.class);
			job.setOutputKeyClass(GraphElement.class);
			job.setOutputValueClass(SetOfStatistics.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);

			return job.isSuccessful() ? 0 : 1;
		}

	}

	public static class ExampleTransform implements Transform {

		private String s;

		@SuppressWarnings("unused")
		public ExampleTransform() { }

		public ExampleTransform(String s) {
			this.s = s;
		}

		@Override
		public GraphElementWithStatistics transform(GraphElementWithStatistics graphElementWithStatistics) {
			if (graphElementWithStatistics.isEntity()) {
				Entity entity = graphElementWithStatistics.getGraphElement().getEntity();
				entity.setSummaryType(s);
				return new GraphElementWithStatistics(new GraphElement(entity), graphElementWithStatistics.getSetOfStatistics());
			}
			Edge edge = graphElementWithStatistics.getGraphElement().getEdge();
			edge.setSummaryType(s);
			return new GraphElementWithStatistics(new GraphElement(edge), graphElementWithStatistics.getSetOfStatistics());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, s);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			s = Text.readString(in);
		}

		@Override
		public String toString() {
			return "ExampleTransform{" +
					"s='" + s + '\'' +
					'}';
		}
	}

	private static Set<GraphElementWithStatistics> getData() {
		// Create set of GraphElementWithStatistics to store data before adding it to the graph.
		Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

		// Edge 1 and some statistics for it
		Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("count", new Count(1));
		data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

		// Edge 2 and some statistics for it
		Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics2 = new SetOfStatistics();
		statistics2.addStatistic("count", new Count(2));
		statistics2.addStatistic("anotherCount", new Count(1000000));
		data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));

		// Edge 3 and some statistics for it
		Edge edge3 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics3 = new SetOfStatistics();
		statistics3.addStatistic("count", new Count(17));
		data.add(new GraphElementWithStatistics(new GraphElement(edge3), statistics3));

		// Edge 4 and some statistics for it
		Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics4 = new SetOfStatistics();
		statistics4.addStatistic("countSomething", new Count(123456));
		data.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

		// Edge 5 and some statistics for it
		Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
		SetOfStatistics statistics5 = new SetOfStatistics();
		statistics5.addStatistic("count", new Count(99));
		data.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

		// Entity 1 and some statistics for it
		Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity1 = new SetOfStatistics();
		statisticsEntity1.addStatistic("entity_count", new Count(1000000));
		data.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

		// Entity 2 and some statistics for it
		Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
		SetOfStatistics statisticsEntity2 = new SetOfStatistics();
		statisticsEntity2.addStatistic("entity_count", new Count(12345));
		data.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

		return data;
	}

}
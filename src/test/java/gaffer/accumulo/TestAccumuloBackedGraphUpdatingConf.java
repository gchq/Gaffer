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
package gaffer.accumulo;

import static org.junit.Assert.assertEquals;

import gaffer.GraphAccessException;
import gaffer.Pair;
import gaffer.accumulo.inputformat.ElementInputFormat;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;
import gaffer.graph.TypeValueRange;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.codehaus.plexus.util.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.*;

/**
 * Unit tests for methods in {@link AccumuloBackedGraph} that update {@link Configuration}s to include
 * the views on the graph.
 *
 * Note that these tests fail unless Accumulo's guava dependency is excluded (see comments in the pom.xml).
 */
public class TestAccumuloBackedGraphUpdatingConf {

    private static final String ROOT = "root";
    private static final String PASSWORD = "";
    private static final String TABLE_NAME = "table";
    private static final Date sevenDaysBefore;
    private static final Date sixDaysBefore;
    private static final Date fiveDaysBefore;
    private static final String visibilityString1 = "";//"private";
    private static final String visibilityString2 = "";//"public";


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
    public void testSetConfigurationWithTypeValuesNoView() throws Exception {
        String instanceName = "testSetConfigurationWithTypeValuesNoView";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setConfiguration(conf, new TypeValue("customer", "A"), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run with no view
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir); // Need to delete this as Junit will create the folder

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 2 edges and 1 entity
        assertEquals(3, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(20));
        statistics1.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
        Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString1, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics2 = new SetOfStatistics();
        statistics2.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));
        Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statisticsEntity1 = new SetOfStatistics();
        statisticsEntity1.addStatistic("entity_count", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

        assertEquals(expectedResults, results);

        // Repeat but also ask for customer B
        driver = new Driver();
        conf = new JobConf();
        Set<TypeValue> typeValues = new HashSet<TypeValue>();
        typeValues.add(new TypeValue("customer", "A"));
        typeValues.add(new TypeValue("customer", "B"));
        graph.setConfiguration(conf, typeValues, accumuloConfig);
        driver.setConf(conf);
        outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        results = new HashSet<GraphElementWithStatistics>();
        count = readResults(fs, new Path(outputDir), results);

        // There should be 3 edges and 1 entity
        assertEquals(4, count);
        Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics5 = new SetOfStatistics();
        statistics5.addStatistic("count", new Count(99));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

        assertEquals(expectedResults, results);
    }

    @Test
    public void testSetConfigurationWithTypeValuesSummaryView() throws Exception {
        String instanceName = "testSetConfigurationWithTypeValuesSummaryView";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setSummaryTypesAndSubTypes(new Pair<String>("purchase", "count"));
        graph.setConfiguration(conf, new TypeValue("customer", "A"), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 2 entities
        assertEquals(1, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statisticsEntity1 = new SetOfStatistics();
        statisticsEntity1.addStatistic("entity_count", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

        assertEquals(expectedResults, results);
        graph.setReturnAllSummaryTypesAndSubTypes();
    }

    @Test
    public void testSetConfigurationWithTypeValuesTimeWindowView() throws Exception {
        String instanceName = "testSetConfigurationWithTypeValuesTimeWindowView";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setTimeWindow(sevenDaysBefore, sixDaysBefore);
        graph.setConfiguration(conf, new TypeValue("customer", "A"), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 entity and 1 edge
        assertEquals(2, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(1));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
        Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statisticsEntity1 = new SetOfStatistics();
        statisticsEntity1.addStatistic("entity_count", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

        assertEquals(expectedResults, results);
        graph.setTimeWindowToAllTime();
    }

    @Test
    public void testSetConfigurationWithTypeValuesAndEntitiesOnly() throws Exception {
        String instanceName = "testSetConfigurationWithTypeValuesAndEntitiesOnly";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setTimeWindow(sevenDaysBefore, sixDaysBefore);
        graph.setReturnEntitiesOnly();
        graph.setConfiguration(conf, new TypeValue("customer", "A"), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 entity
        assertEquals(1, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statisticsEntity1 = new SetOfStatistics();
        statisticsEntity1.addStatistic("entity_count", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

        assertEquals(expectedResults, results);
        graph.setTimeWindowToAllTime();
        graph.setReturnEntitiesAndEdges();
    }

    @Test
    public void testSetConfigurationWithTypeValuesAndEdgesOnly() throws Exception {
        String instanceName = "testSetConfigurationWithTypeValuesAndEdgesOnly";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setTimeWindow(sevenDaysBefore, sixDaysBefore);
        graph.setReturnEdgesOnly();
        graph.setConfiguration(conf, new TypeValue("customer", "A"), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 edge
        assertEquals(1, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(1));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));

        assertEquals(expectedResults, results);
        graph.setTimeWindowToAllTime();
        graph.setReturnEntitiesAndEdges();
    }

    @Test
    public void testSetConfigurationWithTypeValueRanges() throws Exception {
        String instanceName = "testSetConfigurationWithTypeValueRanges";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        TypeValueRange range = new TypeValueRange("customer", "A", "customer", "Z");
        graph.setConfigurationFromRanges(conf, range, accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 3 edges and 1 entity
        assertEquals(4, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(20));
        statistics1.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
        Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics4 = new SetOfStatistics();
        statistics4.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));
        Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics5 = new SetOfStatistics();
        statistics5.addStatistic("count", new Count(99));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));
        Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statisticsEntity1 = new SetOfStatistics();
        statisticsEntity1.addStatistic("entity_count", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));

        assertEquals(expectedResults, results);
    }

    @Test
    public void testSetConfigurationWithPairs() throws Exception {
        String instanceName = "testSetConfigurationWithPairs";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        Pair<TypeValue> pair = new Pair<TypeValue>(new TypeValue("customer", "B"), new TypeValue("product", "Q"));
        graph.setConfigurationFromPairs(conf, pair, accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 edge
        assertEquals(1, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics5 = new SetOfStatistics();
        statistics5.addStatistic("count", new Count(99));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

        assertEquals(expectedResults, results);
    }

    @Test
    public void testSetConfigurationWithRollUpOff() throws Exception {
        String instanceName = "testSetConfigurationWithRollUpOff";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        graph.rollUpOverTimeAndVisibility(false);
        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        Pair<TypeValue> pair = new Pair<TypeValue>(new TypeValue("customer", "A"), new TypeValue("product", "P"));
        graph.setConfigurationFromPairs(conf, pair, accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 3 edges
        assertEquals(3, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(1));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
        Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics2 = new SetOfStatistics();
        statistics2.addStatistic("count", new Count(19));
        statistics2.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics2));
        Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics4 = new SetOfStatistics();
        statistics4.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

        assertEquals(expectedResults, results);
    }

    @Test
    public void testSetConfigurationFullTableScan() throws Exception {
        String instanceName = "testSetConfigurationFullTableScan";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setConfiguration(conf, accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 3 edges and 2 entities
        assertEquals(5, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        // Edge 1 and some statistics for it
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(20));
        statistics1.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
        Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics4 = new SetOfStatistics();
        statistics4.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));
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

        assertEquals(expectedResults, results);
    }

    @Test
    public void testSetConfigurationDirectedEdgesOnly() throws Exception {
        String instanceName = "testSetConfigurationDirectedEdgesOnly";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setDirectedEdgesOnly();
        graph.setConfiguration(conf, accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 2 edges and 2 entities
        assertEquals(4, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        // Edge 1 and some statistics for it
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(20));
        statistics1.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
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

        assertEquals(expectedResults, results);
    }

    @Test
    public void testSetConfigurationUndirectedEdgesOnly() throws Exception {
        String instanceName = "testSetConfigurationUndirectedEdgesOnly";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setUndirectedEdgesOnly();
        graph.setConfiguration(conf, accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 edge and 2 entities
        assertEquals(3, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        // Edge 4 and some statistics for it
        Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics4 = new SetOfStatistics();
        statistics4.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));
        Entity entity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statisticsEntity1 = new SetOfStatistics();
        statisticsEntity1.addStatistic("entity_count", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity1), statisticsEntity1));
        Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statisticsEntity2 = new SetOfStatistics();
        statisticsEntity2.addStatistic("entity_count", new Count(12345));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

        assertEquals(expectedResults, results);
    }

    @Test
    public void testSetConfigurationUndirectedEdgesOnlyNoEntities() throws Exception {
        String instanceName = "testSetConfigurationUndirectedEdgesOnlyNoEntities";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setUndirectedEdgesOnly();
        graph.setReturnEdgesOnly();
        graph.setConfiguration(conf, accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 edge
        assertEquals(1, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        // Edge 4 and some statistics for it
        Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics4 = new SetOfStatistics();
        statistics4.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));

        assertEquals(expectedResults, results);
    }

    /**
     * Tests that calling {@link AccumuloBackedGraph#setOutgoingEdgesOnly} doesn't cause any problems
     * with full table scans (where that option should be ignored as there is nothing to be outgoing
     * from).
     *
     * @throws Exception
     */
    @Test
    public void testSetConfigurationOutgoingEdgesOnlyFullTable() throws Exception {
        String instanceName = "testSetConfigurationOutgoingEdgesOnlyFullTable";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setOutgoingEdgesOnly();
        graph.setConfiguration(conf, accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 3 edges and 2 entities
        assertEquals(5, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        // Edge 1 and some statistics for it
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(20));
        statistics1.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
        Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics4 = new SetOfStatistics();
        statistics4.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));
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

        assertEquals(expectedResults, results);
    }

    /**
     * Tests that calling {@link AccumuloBackedGraph#setOutgoingEdgesOnly} is correctly applied to
     * confs when a query range is specified.
     *
     * @throws Exception
     */
    @Test
    public void testSetConfigurationOutgoingEdgesOnlyQuery() throws Exception {
        String instanceName = "testSetConfigurationOutgoingEdgesOnlyQuery";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        // First query for customer|B: should find the edge customer|B -> product|Q as that
        // is outgoing from B
        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setOutgoingEdgesOnly();
        graph.setConfiguration(conf, new TypeValue("customer", "B"), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 edge
        assertEquals(1, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics5 = new SetOfStatistics();
        statistics5.addStatistic("count", new Count(99));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

        assertEquals(expectedResults, results);

        // Now query for product|Q: should find no edges (as there are no outgoing edges from Q)
        conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setOutgoingEdgesOnly();
        graph.setConfiguration(conf, new TypeValue("product", "Q"), accumuloConfig);

        // Run
        driver = new Driver();
        driver.setConf(conf);
        outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        results = new HashSet<GraphElementWithStatistics>();
        count = readResults(fs, new Path(outputDir), results);

        // There should be no results
        assertEquals(0, count);
    }

    @Test
    public void testSetConfigurationOutgoingEdgesOnlyQueryFromRanges() throws Exception {
        String instanceName = "testSetConfigurationOutgoingEdgesOnlyQueryFromRanges";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        // First query for range of all customers - should find edges A->B, A->P
        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setReturnEdgesOnly();
        graph.setOutgoingEdgesOnly();
        graph.setConfigurationFromRanges(conf, new TypeValueRange("customer", "", "customer", "Z"), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 3 edges
        assertEquals(3, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(20));
        statistics1.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
        Edge edge4 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics4 = new SetOfStatistics();
        statistics4.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge4), statistics4));
        Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics5 = new SetOfStatistics();
        statistics5.addStatistic("count", new Count(99));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

        assertEquals(expectedResults, results);

        // Now query for all products should find no edges (as there are no outgoing directed edges from products)
        conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setReturnEdgesOnly();
        graph.setDirectedEdgesOnly();
        graph.setOutgoingEdgesOnly();
        graph.setConfigurationFromRanges(conf, new TypeValueRange("product", "", "product", "Z"), accumuloConfig);

        // Run
        driver = new Driver();
        driver.setConf(conf);
        outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        results = new HashSet<GraphElementWithStatistics>();
        count = readResults(fs, new Path(outputDir), results);

        // There should be no results
        assertEquals(0, count);
    }

    @Test
    public void testSetConfigurationOutgoingEdgesOnlyQueryFromPairs() throws Exception {
        String instanceName = "testSetConfigurationOutgoingEdgesOnlyQueryFromPairs";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        // First query for pair customer|B, product|Q - should find edge B->Q
        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setOutgoingEdgesOnly();
        graph.setConfigurationFromPairs(conf, new Pair<TypeValue>(new TypeValue("customer", "B"), new TypeValue("product", "Q")), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 edge
        assertEquals(1, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics5 = new SetOfStatistics();
        statistics5.addStatistic("count", new Count(99));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

        assertEquals(expectedResults, results);

        // Now query for pair product|Q, customer|B - shouldn't find any edges
        conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setOutgoingEdgesOnly();
        graph.setConfigurationFromPairs(conf, new Pair<TypeValue>(new TypeValue("product", "Q"), new TypeValue("customer", "B")), accumuloConfig);

        // Run
        driver = new Driver();
        driver.setConf(conf);
        outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        results = new HashSet<GraphElementWithStatistics>();
        count = readResults(fs, new Path(outputDir), results);

        // There should be no results
        assertEquals(0, count);
    }

    @Test
    public void testSetConfigurationIncomingEdgesOnlyQueryFromPairs() throws Exception {
        String instanceName = "testSetConfigurationIncomingEdgesOnlyQueryFromPairs";
        AccumuloBackedGraph graph = setUpGraphAndMockAccumulo(instanceName);
        AccumuloConfig accumuloConfig = setUpAccumuloConfig(instanceName);

        // First query for pair product|Q, customer|B - should find edge B->Q
        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setIncomingEdgesOnly();
        graph.setConfigurationFromPairs(conf, new Pair<TypeValue>(new TypeValue("product", "Q"), new TypeValue("customer", "B")), accumuloConfig);
        FileSystem fs = FileSystem.getLocal(conf);

        // Run
        Driver driver = new Driver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        int count = readResults(fs, new Path(outputDir), results);

        // There should be 1 edge
        assertEquals(1, count);

        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();

        Edge edge5 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics5 = new SetOfStatistics();
        statistics5.addStatistic("count", new Count(99));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge5), statistics5));

        assertEquals(expectedResults, results);

        // Now query for pair customer|B, product|Q - shouldn't find any edges
        conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        graph.setIncomingEdgesOnly();
        graph.setConfigurationFromPairs(conf, new Pair<TypeValue>(new TypeValue("customer", "B"), new TypeValue("product", "Q")), accumuloConfig);

        // Run
        driver = new Driver();
        driver.setConf(conf);
        outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        assertEquals(0, driver.run(new String[]{outputDir}));

        // Read results in
        results = new HashSet<GraphElementWithStatistics>();
        count = readResults(fs, new Path(outputDir), results);

        // There should be no results
        assertEquals(0, count);
    }

    public static int readResults(FileSystem fs, Path path, Set<GraphElementWithStatistics> results) throws IOException {
        FileStatus[] fileStatus = fs.listStatus(path, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().contains("part-m-");
            }
        });
        int count = 0;
        for (int i = 0; i < fileStatus.length; i++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, fileStatus[i].getPath(), fs.getConf());
            GraphElement element = new GraphElement();
            SetOfStatistics statistics = new SetOfStatistics();
            while (reader.next(element, statistics)) {
                count++;
                results.add(new GraphElementWithStatistics(element.clone(), statistics.clone()));
            }
            reader.close();
        }
        return count;
    }

    public static AccumuloConfig setUpAccumuloConfig(String instanceName) {
        AccumuloConfig accumuloConfig = new AccumuloConfig();
        accumuloConfig.setInstanceName(instanceName);
        accumuloConfig.setTable(TABLE_NAME);
        accumuloConfig.setUserName(ROOT);
        accumuloConfig.setPassword(PASSWORD);
        return accumuloConfig;
    }

    public static AccumuloBackedGraph setUpGraphAndMockAccumulo(String instanceName) throws AccumuloSecurityException,
            AccumuloException, TableExistsException, TableNotFoundException, GraphAccessException {
        MockInstance mockInstance = new MockInstance(instanceName);
        Connector connector = mockInstance.getConnector(ROOT, new PasswordToken(""));
        TableUtils.createTable(connector, TABLE_NAME, 30 * 24 * 60 * 60 * 1000L);
        AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, TABLE_NAME);
        graph.addGraphElementsWithStatistics(getData());
        return graph;
    }

    class Driver extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            if (args.length != 1) {
                throw new IllegalArgumentException("Usage : " + Driver.class.getName() + " <output>");
            }

            String outputDir = args[0];

            Job job = new Job(getConf());
            job.setJarByClass(this.getClass());

            job.setInputFormatClass(ElementInputFormat.class);

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

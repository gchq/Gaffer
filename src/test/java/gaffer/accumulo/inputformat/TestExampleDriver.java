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

import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.TableUtils;
import gaffer.accumulo.inputformat.example.ExampleDriver;
import gaffer.accumulo.utils.AccumuloConfig;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.plexus.util.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ExampleDriver}.
 */
public class TestExampleDriver {

    private static Date sevenDaysBefore;
    private static Date sixDaysBefore;
    private static Date fiveDaysBefore;
    private static String visibilityString1 = "";//"private";
    private static String visibilityString2 = "";//"public";

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
    public void testFullTableScan() throws Exception {
        String instanceName = "testFullTableScan";
        String tableName = "table" + instanceName;
        MockInstance mockInstance = new MockInstance(instanceName);
        Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
        TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
        Set<GraphElementWithStatistics> data = getData();
        AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
        graph.addGraphElementsWithStatistics(data);

        String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
        BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
        bw.write("accumulo.instance=" + instanceName + "\n");
        bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
        bw.write("accumulo.table=" + tableName + "\n");
        bw.write("accumulo.user=root\n");
        bw.write("accumulo.password=\n");
        bw.close();

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        FileSystem fs = FileSystem.getLocal(conf);

        ExampleDriver driver = new ExampleDriver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);

        driver.run(new String[]{outputDir, accumuloPropertiesFilename, "0", "null", "null", "true"});

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
        Edge edge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        SetOfStatistics statistics1 = new SetOfStatistics();
        statistics1.addStatistic("count", new Count(20));
        statistics1.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics1));
        Edge edge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString1, sixDaysBefore, fiveDaysBefore);
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
    public void testReadOneSeed() throws Exception {
        String instanceName = "testReadOneSeed";
        String tableName = "table" + instanceName;
        MockInstance mockInstance = new MockInstance(instanceName);
        Connector connector = mockInstance.getConnector("root", new PasswordToken(""));
        TableUtils.createTable(connector, tableName, 30 * 24 * 60 * 60 * 1000L);
        Set<GraphElementWithStatistics> data = getData();
        AccumuloBackedGraph graph = new AccumuloBackedGraph(connector, tableName);
        graph.addGraphElementsWithStatistics(data);

        String accumuloPropertiesFilename = tempFolder.newFile().getAbsolutePath();
        BufferedWriter bw = new BufferedWriter(new FileWriter(accumuloPropertiesFilename));
        bw.write("accumulo.instance=" + instanceName + "\n");
        bw.write("accumulo.zookeepers=" + AccumuloConfig.MOCK_ZOOKEEPERS + "\n");
        bw.write("accumulo.table=" + tableName + "\n");
        bw.write("accumulo.user=root\n");
        bw.write("accumulo.password=\n");
        bw.close();

        String seedFilename = tempFolder.newFile().getAbsolutePath();
        bw = new BufferedWriter(new FileWriter(seedFilename));
        bw.write("product|R\n");
        bw.close();

        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        FileSystem fs = FileSystem.getLocal(conf);

        ExampleDriver driver = new ExampleDriver();
        driver.setConf(conf);
        String outputDir = tempFolder.newFolder().getAbsolutePath();
        FileUtils.deleteDirectory(outputDir);
        driver.run(new String[]{outputDir, accumuloPropertiesFilename, "0", "null", "null", "true", seedFilename});

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
        // There should be 1 entity
        assertEquals(1, count);
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Entity entity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statisticsEntity2 = new SetOfStatistics();
        statisticsEntity2.addStatistic("entity_count", new Count(12345));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity2), statisticsEntity2));

        assertEquals(results, expectedResults);
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

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
package gaffer.accumulo.iterators;

import gaffer.GraphAccessException;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.TableUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import gaffer.statistics.impl.IntSet;
import gaffer.statistics.impl.MinuteBitMap;
import gaffer.statistics.transform.StatisticsTransform;
import gaffer.statistics.transform.impl.StatisticsRemoverByName;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit test of the {@link StatisticsTransform} iterator.
 */
public class TestStatisticTransformIterator {

    @Test
    public void test() {
        Instance instance = new MockInstance();
        String tableName = "Test";
        String visibilityString = "public";
        long ageOffTimeInMilliseconds = 30 * 24 * 60 * 60 * 1000L; // 30 days in milliseconds

        long currentTime = System.currentTimeMillis();
        Calendar currentCalendar = new GregorianCalendar();
        currentCalendar.setTime(new Date(currentTime));

        try {
            // Open connection
            Connector conn = instance.getConnector("user", "password");

            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
            TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

            // Edge to put in table (from a week before the current date)
            Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
            sevenDaysBeforeCalendar.setTime(new Date(currentTime));
            sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
            Date sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
            sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
            Date sixDaysBefore = sevenDaysBeforeCalendar.getTime();

            // Create an edge and an entity with different statistics
            Edge edge = new Edge("customer", "A", "product", "B", "purchase", "instore", true, visibilityString, sevenDaysBefore, sixDaysBefore);
            SetOfStatistics statistics1 = new SetOfStatistics("count", new Count(1));
            statistics1.addStatistic("times", new MinuteBitMap(sixDaysBefore));
            statistics1.addStatistic("intSet", new IntSet(1, 2, 3));
            Entity entity = new Entity("customer", "A", "purchase", "instore", visibilityString, sevenDaysBefore, sixDaysBefore);
            SetOfStatistics statistics2 = new SetOfStatistics("count", new Count(100));

            // Create set of data
            Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();
            data.add(new GraphElementWithStatistics(new GraphElement(edge), statistics1));
            data.add(new GraphElementWithStatistics(new GraphElement(entity), statistics2));

            // Create Accumulo backed graph
            AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

            // Add data
            graph.addGraphElementsWithStatistics(data);

            // Set graph up for query
            Authorizations authorizations = new Authorizations(visibilityString);

            // Set only want statistics count and times
            Set<String> wantedStats = new HashSet<String>();
            wantedStats.add("count");
            wantedStats.add("times");
            StatisticsTransform transform = new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.KEEP, wantedStats);
            org.apache.accumulo.core.client.Scanner scanner = conn.createScanner(tableName, authorizations);
            TableUtils.addStatisticsTransformToScanner(scanner, transform);
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            Map<GraphElement, SetOfStatistics> results = new HashMap<GraphElement, SetOfStatistics>();
            while (it.hasNext()) {
                Map.Entry<Key, Value> entry = it.next();
                GraphElement element = ConversionUtils.getGraphElementFromKey(entry.getKey());
                SetOfStatistics filteredStatistics = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
                results.put(element, filteredStatistics);
            }
            scanner.close();
            assertEquals(2, results.size());
            SetOfStatistics filteredEdgeStatistics = results.get(new GraphElement(edge));
            assertEquals(wantedStats, filteredEdgeStatistics.getStatistics().keySet());
            filteredEdgeStatistics = results.get(new GraphElement(entity));
            assertEquals(new HashSet<String>(Collections.singleton("count")), filteredEdgeStatistics.getStatistics().keySet());
        } catch (AccumuloException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (AccumuloSecurityException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (TableExistsException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (IOException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (GraphAccessException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void testWhenAskForNoStats() {
        Instance instance = new MockInstance();
        String tableName = "Test";
        String visibilityString = "public";
        long ageOffTimeInMilliseconds = 30 * 24 * 60 * 60 * 1000L; // 30 days in milliseconds

        long currentTime = System.currentTimeMillis();
        Calendar currentCalendar = new GregorianCalendar();
        currentCalendar.setTime(new Date(currentTime));

        try {
            // Open connection
            Connector conn = instance.getConnector("user", "password");

            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
            TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

            // Edge to put in table (from a week before the current date)
            Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
            sevenDaysBeforeCalendar.setTime(new Date(currentTime));
            sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
            Date sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
            sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
            Date sixDaysBefore = sevenDaysBeforeCalendar.getTime();

            // Create an edge and an entity with different statistics
            Edge edge = new Edge("customer", "A", "product", "B", "purchase", "instore", true, visibilityString, sevenDaysBefore, sixDaysBefore);
            SetOfStatistics statistics1 = new SetOfStatistics("count", new Count(1));
            statistics1.addStatistic("times", new MinuteBitMap(sixDaysBefore));
            statistics1.addStatistic("intSet", new IntSet(1, 2, 3));
            Entity entity = new Entity("customer", "A", "purchase", "instore", visibilityString, sevenDaysBefore, sixDaysBefore);
            SetOfStatistics statistics2 = new SetOfStatistics("count", new Count(100));

            // Create set of data
            Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();
            data.add(new GraphElementWithStatistics(new GraphElement(edge), statistics1));
            data.add(new GraphElementWithStatistics(new GraphElement(entity), statistics2));

            // Create Accumulo backed graph
            AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

            // Add data
            graph.addGraphElementsWithStatistics(data);

            // Set graph up for query
            Authorizations authorizations = new Authorizations(visibilityString);

            // Set want no statistics
            Set<String> wantedStats = new HashSet<String>();
            org.apache.accumulo.core.client.Scanner scanner = conn.createScanner(tableName, authorizations);
            StatisticsTransform transform = new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.KEEP, wantedStats);
            TableUtils.addStatisticsTransformToScanner(scanner, transform);
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            Map<GraphElement, SetOfStatistics> results = new HashMap<GraphElement, SetOfStatistics>();
            while (it.hasNext()) {
                Map.Entry<Key, Value> entry = it.next();
                GraphElement element = ConversionUtils.getGraphElementFromKey(entry.getKey());
                SetOfStatistics filteredStatistics = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
                results.put(element, filteredStatistics);
            }
            scanner.close();
            assertEquals(2, results.size());
            SetOfStatistics filteredEdgeStatistics = results.get(new GraphElement(edge));
            assertEquals(wantedStats, filteredEdgeStatistics.getStatistics().keySet());
            filteredEdgeStatistics = results.get(new GraphElement(entity));
            assertEquals(wantedStats, filteredEdgeStatistics.getStatistics().keySet());
        } catch (AccumuloException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (AccumuloSecurityException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (TableExistsException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (IOException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        } catch (GraphAccessException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

}

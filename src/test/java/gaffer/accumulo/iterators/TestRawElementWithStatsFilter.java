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
import gaffer.Pair;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.TableUtils;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.accumulo.predicate.RawGraphElementWithStatisticsPredicate;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.Predicate;
import gaffer.predicate.summarytype.SummaryTypePredicate;
import gaffer.predicate.summarytype.impl.CombinedPredicates;
import gaffer.predicate.summarytype.impl.RegularExpressionPredicate;
import gaffer.predicate.summarytype.impl.SummaryTypeAndSubTypeInSetPredicate;
import gaffer.predicate.summarytype.impl.SummaryTypeInSetPredicate;
import gaffer.predicate.time.impl.TimeWindowPredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Unit test for {@link RawElementWithStatsFilter}.
 */
public class TestRawElementWithStatsFilter {

    @Test
    public void test() throws Exception {
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

            // Create some entities and edges
            Edge edge1 = new Edge("customer", "A", "product", "B", "purchase", "instore", true, visibilityString, sevenDaysBefore, sixDaysBefore);
            Edge edge2 = new Edge("customer", "A", "product", "B", "purchase2", "outstore", false, visibilityString, sevenDaysBefore, sixDaysBefore);
            Entity entity = new Entity("customer", "A", "purchase3", "outstore", visibilityString, sevenDaysBefore, sixDaysBefore);

            // Create some SetOfStatistics
            SetOfStatistics statistics = new SetOfStatistics();
            statistics.addStatistic("count", new Count(1));

            // Create set of data
            Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();
            data.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics));
            data.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics));
            data.add(new GraphElementWithStatistics(new GraphElement(entity), statistics));

            // Create Accumulo backed graph
            AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

            // Add data
            graph.addGraphElementsWithStatistics(data);

            // Set graph up for query
            Authorizations authorizations = new Authorizations(visibilityString);

            // Filter on summary type being "purchase" - will get the same edge twice (as goes into Accumulo in both directions)
            SummaryTypePredicate predicate = new SummaryTypeInSetPredicate("purchase");
            org.apache.accumulo.core.client.Scanner scanner = conn.createScanner(tableName, authorizations);
            TableUtils.addPreRollUpFilterIteratorToScanner(scanner, new RawGraphElementWithStatisticsPredicate(predicate));
            Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
            Map.Entry<Key,Value> entry = it.next();
            GraphElement element = ConversionUtils.getGraphElementFromKey(entry.getKey());
            assertTrue(element.isEdge());
            assertEquals(edge1, element.getEdge());
            entry = it.next();
            element = ConversionUtils.getGraphElementFromKey(entry.getKey());
            assertTrue(element.isEdge());
            assertEquals(edge1, element.getEdge());
            assertFalse(it.hasNext());
            scanner.close();

            // Filter on summary type being "purchase" or "purchase3"
            Set<String> summaryTypes = new HashSet<String>();
            summaryTypes.add("purchase");
            summaryTypes.add("purchase3");
            predicate = new SummaryTypeInSetPredicate(summaryTypes);
            scanner = conn.createScanner(tableName, authorizations);
            TableUtils.addPreRollUpFilterIteratorToScanner(scanner, new RawGraphElementWithStatisticsPredicate(predicate));
            Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics));
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity), statistics));
            Set<GraphElementWithStatistics> foundResults = new HashSet<GraphElementWithStatistics>();
            it = scanner.iterator();
            while (it.hasNext()) {
                Map.Entry<Key, Value> readEntry = it.next();
                GraphElement readElement = ConversionUtils.getGraphElementFromKey(readEntry.getKey());
                SetOfStatistics readStatistics = ConversionUtils.getSetOfStatisticsFromValue(readEntry.getValue());
                foundResults.add(new GraphElementWithStatistics(readElement, readStatistics));
            }
            assertEquals(expectedResults, foundResults);
            scanner.close();

            // Filter on summary type being "nothing"
            predicate = new SummaryTypeInSetPredicate("nothing");
            scanner = conn.createScanner(tableName, authorizations);
            TableUtils.addPreRollUpFilterIteratorToScanner(scanner, new RawGraphElementWithStatisticsPredicate(predicate));
            it = scanner.iterator();
            assertFalse(it.hasNext());
            scanner.close();

            // Filter on summary type being "purchase" and subtype being "instore" - will get the
            // same edge twice (as goes into Accumulo in both directions)
            Set<Pair<String>> summaryTypesAndSubtypes = new HashSet<Pair<String>>();
            summaryTypesAndSubtypes.add(new Pair<String>("purchase", "instore"));
            predicate = new SummaryTypeAndSubTypeInSetPredicate(summaryTypesAndSubtypes);
            scanner = conn.createScanner(tableName, authorizations);
            TableUtils.addPreRollUpFilterIteratorToScanner(scanner, new RawGraphElementWithStatisticsPredicate(predicate));
            it = scanner.iterator();
            entry = it.next();
            element = ConversionUtils.getGraphElementFromKey(entry.getKey());
            assertTrue(element.isEdge());
            assertEquals(edge1, element.getEdge());
            entry = it.next();
            element = ConversionUtils.getGraphElementFromKey(entry.getKey());
            assertTrue(element.isEdge());
            assertEquals(edge1, element.getEdge());
            assertFalse(it.hasNext());
            scanner.close();

            // Filter on summary type being "purchase" or summary type being purchase3 and subtype being outstore
            SummaryTypePredicate predicate1 = new SummaryTypeInSetPredicate("purchase");
            summaryTypesAndSubtypes = new HashSet<Pair<String>>();
            summaryTypesAndSubtypes.add(new Pair<String>("purchase3", "outstore"));
            SummaryTypePredicate predicate2 = new SummaryTypeAndSubTypeInSetPredicate(summaryTypesAndSubtypes);
            predicate = new CombinedPredicates(predicate1, predicate2, CombinedPredicates.Combine.OR);
            scanner = conn.createScanner(tableName, authorizations);
            TableUtils.addPreRollUpFilterIteratorToScanner(scanner, new RawGraphElementWithStatisticsPredicate(predicate));
            expectedResults = new HashSet<GraphElementWithStatistics>();
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), statistics));
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity), statistics));
            foundResults = new HashSet<GraphElementWithStatistics>();
            it = scanner.iterator();
            while (it.hasNext()) {
                Map.Entry<Key, Value> readEntry = it.next();
                GraphElement readElement = ConversionUtils.getGraphElementFromKey(readEntry.getKey());
                SetOfStatistics readStatistics = ConversionUtils.getSetOfStatisticsFromValue(readEntry.getValue());
                foundResults.add(new GraphElementWithStatistics(readElement, readStatistics));
            }
            assertEquals(expectedResults, foundResults);
            scanner.close();

            // Filter on summary type matching pur*
            Pattern summaryTypePattern = Pattern.compile("pur.");
            Pattern summarySubTypePattern = Pattern.compile("out.");
            predicate = new RegularExpressionPredicate(summaryTypePattern, summarySubTypePattern);
            scanner = conn.createScanner(tableName, authorizations);
            TableUtils.addPreRollUpFilterIteratorToScanner(scanner, new RawGraphElementWithStatisticsPredicate(predicate));
            expectedResults = new HashSet<GraphElementWithStatistics>();
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge2), statistics));
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(entity), statistics));
            foundResults = new HashSet<GraphElementWithStatistics>();
            it = scanner.iterator();
            while (it.hasNext()) {
                Map.Entry<Key, Value> readEntry = it.next();
                GraphElement readElement = ConversionUtils.getGraphElementFromKey(readEntry.getKey());
                SetOfStatistics readStatistics = ConversionUtils.getSetOfStatisticsFromValue(readEntry.getValue());
                foundResults.add(new GraphElementWithStatistics(readElement, readStatistics));
            }
            assertEquals(expectedResults, foundResults);
            scanner.close();

            // Filter on summary type being "purchase" and time window being ancient - should get nothing
            predicate = new SummaryTypeInSetPredicate("purchase");
            Predicate<RawGraphElementWithStatistics> combinedPredicate = new gaffer.predicate.CombinedPredicates(new RawGraphElementWithStatisticsPredicate(predicate),
                    new RawGraphElementWithStatisticsPredicate(new TimeWindowPredicate(new Date(0L), new Date(1000L))), gaffer.predicate.CombinedPredicates.Combine.AND);
            scanner = conn.createScanner(tableName, authorizations);
            TableUtils.addPreRollUpFilterIteratorToScanner(scanner, combinedPredicate);
            it = scanner.iterator();
            if (it.hasNext()) {
                fail("Shouldn't have found any results");
            }

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
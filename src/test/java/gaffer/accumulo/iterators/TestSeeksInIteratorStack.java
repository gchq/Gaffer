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

import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.TableUtils;
import gaffer.accumulo.predicate.impl.OutgoingEdgePredicate;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import gaffer.statistics.transform.impl.StatisticsRemoverByName;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests that <code>seek</code>s within the iterator stack are handled correctly.
 */
public class TestSeeksInIteratorStack {

    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
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


    @Test
    public void testSeeks() throws IOException {
        long ageOffTimeInMilliseconds = 30 * 24 * 60 * 60 * 1000L;

        TreeMap<Key,Value> tm1 = setupGraph();

        // Create iterator settings
        //  - Age-off
        IteratorSetting ageOffIS = TableUtils.getAgeOffIteratorSetting(ageOffTimeInMilliseconds);
        //  - Combiner
        IteratorSetting combinerIS = TableUtils.getSetOfStatisticsCombinerIteratorSetting();
        //  - Pre roll-up filter
        IteratorSetting preRollUpFilterIS = TableUtils.getPreRollUpFilterIteratorSetting(new OutgoingEdgePredicate());
        //  - Statistics transform filter
        IteratorSetting statisticsTransformFilter = TableUtils.getStatisticTransformIteratorSetting(new StatisticsRemoverByName(StatisticsRemoverByName.KeepRemove.REMOVE, "nothing"));
        //  - Roll-up iterator
        IteratorSetting rollUpIteratorSetting = TableUtils.getRollUpOverTimeAndVisibilityIteratorSetting();

        // Create stack
        SortedMapIterator data = new SortedMapIterator(tm1);
        AgeOffFilter ageOffFilter = new AgeOffFilter();
        ageOffFilter.init(data, ageOffIS.getOptions(), null);
        SetOfStatisticsCombiner combiner = new SetOfStatisticsCombiner();
        combiner.init(ageOffFilter, combinerIS.getOptions(), null);
        RawElementWithStatsFilter preRollUpFilter = new RawElementWithStatsFilter();
        preRollUpFilter.init(combiner, preRollUpFilterIS.getOptions(), null);
        StatisticTransformIterator statsTransformIt = new StatisticTransformIterator();
        statsTransformIt.init(preRollUpFilter, statisticsTransformFilter.getOptions(), null);
        RollUpOverTimeAndVisibility rollUp = new RollUpOverTimeAndVisibility();
        rollUp.init(statsTransformIt, rollUpIteratorSetting.getOptions(), null);

        // Seek to a range covering everything - should get all data, rolled up
        rollUp.seek(new Range(), EMPTY_COL_FAMS, false);
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        while (rollUp.hasTop()) {
            Key key = rollUp.getTopKey();
            Value value = rollUp.getTopValue();
            GraphElement element = ConversionUtils.getGraphElementFromKey(key);
            SetOfStatistics statistics = ConversionUtils.getSetOfStatisticsFromValue(value);
            results.add(new GraphElementWithStatistics(element, statistics));
            rollUp.next();
        }
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        Edge expectedEdge1 = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1 + "&" + visibilityString2, sevenDaysBefore, fiveDaysBefore);
        SetOfStatistics expectedStatistics1 = new SetOfStatistics();
        expectedStatistics1.addStatistic("count", new Count(20));
        expectedStatistics1.addStatistic("anotherCount", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(expectedEdge1), expectedStatistics1));
        Edge expectedEdge2 = new Edge("customer", "A", "product", "P", "purchase", "instore", false, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics expectedStatistics2 = new SetOfStatistics();
        expectedStatistics2.addStatistic("countSomething", new Count(123456));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(expectedEdge2), expectedStatistics2));
        Edge expectedEdge3 = new Edge("customer", "B", "product", "Q", "purchase", "instore", true, visibilityString2, sixDaysBefore, fiveDaysBefore);
        SetOfStatistics expectedStatistics3 = new SetOfStatistics();
        expectedStatistics3.addStatistic("count", new Count(99));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(expectedEdge3), expectedStatistics3));
        Entity expectedEntity1 = new Entity("customer", "A", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics expectedStatistics4 = new SetOfStatistics();
        expectedStatistics4.addStatistic("entity_count", new Count(1000000));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(expectedEntity1), expectedStatistics4));
        Entity expectedEntity2 = new Entity("product", "R", "purchase", "count", visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics expectedStatistics5 = new SetOfStatistics();
        expectedStatistics5.addStatistic("entity_count", new Count(12345));
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(expectedEntity2), expectedStatistics5));
        assertEquals(expectedResults, results);

        // Seek to a range containing nothing and check get nothing
        rollUp.seek(Range.exact("XXXXXX"), EMPTY_COL_FAMS, false);
        if (rollUp.hasTop()) {
            fail("Shouldn't have top");
        }

        // Seek to the middle of a row
        Edge edge = new Edge("customer", "A", "product", "P", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        Key k = new Key(new Text(ConversionUtils.getRowKeysFromEdge(edge).getFirst()), new Text(ConversionUtils.getColumnFamilyFromEdge(edge)),
                new Text(""));
        Edge edge2 = new Edge("customer", "A", "product", "Z", "purchase", "instore", true, visibilityString1, sevenDaysBefore, fiveDaysBefore);
        Key key2 = ConversionUtils.getKeysFromEdge(edge2).getFirst();
        Range range = new Range(k, true, key2, false);
        rollUp.seek(range, EMPTY_COL_FAMS, false);
        results.clear();
        while (rollUp.hasTop()) {
            Key key = rollUp.getTopKey();
            Value value = rollUp.getTopValue();
            GraphElement element = ConversionUtils.getGraphElementFromKey(key);
            SetOfStatistics statistics = ConversionUtils.getSetOfStatisticsFromValue(value);
            results.add(new GraphElementWithStatistics(element, statistics));
            rollUp.next();
        }
        expectedResults.clear();
        expectedResults.add(new GraphElementWithStatistics(new GraphElement(expectedEdge1), expectedStatistics1));
        assertEquals(expectedResults, results);
    }

    private static TreeMap<Key, Value> setupGraph() {
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

        // Add key-values to map
        TreeMap<Key, Value> map = new TreeMap<Key, Value>();
        for (GraphElementWithStatistics gews : data) {
            map.put(ConversionUtils.getKeysFromGraphElement(gews.getGraphElement()).getFirst(),
                   ConversionUtils.getValueFromSetOfStatistics(gews.getSetOfStatistics()));
            }
        return map;
    }

}

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
package gaffer.accumulo.predicate;

import gaffer.accumulo.ConversionUtils;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.CombinedPredicates;
import gaffer.predicate.Predicate;
import gaffer.predicate.graph.impl.IsEntityPredicate;
import gaffer.predicate.summarytype.SummaryTypePredicate;
import gaffer.predicate.summarytype.impl.SummaryTypeInSetPredicate;
import gaffer.predicate.time.impl.TimeWindowPredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.junit.Test;

import java.io.*;
import java.util.Collections;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link RawGraphElementWithStatisticsPredicate}. Contains tests for writing and reading, equals
 * and accept.
 */
public class TestRawGraphElementWithStatisticsPredicate {

    @Test
    public void testWriteRead() throws IOException {
        // Simple RawGraphElementWithStatisticsPredicate
        Predicate<GraphElementWithStatistics> predicate = new TimeWindowPredicate(new Date(0L), new Date(100L));
        RawGraphElementWithStatisticsPredicate rawPredicate = new RawGraphElementWithStatisticsPredicate(predicate);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        rawPredicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        RawGraphElementWithStatisticsPredicate read = new RawGraphElementWithStatisticsPredicate();
        read.readFields(in);
        assertEquals(read, rawPredicate);

        // Composite RawGraphElementWithStatisticsPredicate
        SummaryTypePredicate summaryTypePredicate = new SummaryTypeInSetPredicate(Collections.singleton("abc"));
        Predicate<GraphElementWithStatistics> predicate2 = new CombinedPredicates(predicate, summaryTypePredicate, CombinedPredicates.Combine.OR);
        rawPredicate = new RawGraphElementWithStatisticsPredicate(predicate2);
        baos = new ByteArrayOutputStream();
        out = new DataOutputStream(baos);
        rawPredicate.write(out);
        bais = new ByteArrayInputStream(baos.toByteArray());
        in = new DataInputStream(bais);
        read = new RawGraphElementWithStatisticsPredicate();
        read.readFields(in);
        assertEquals(read, rawPredicate);

        // Another composite RawGraphElementWithStatisticsPredicate
        Predicate<GraphElementWithStatistics> predicate3 = new CombinedPredicates(predicate2, new IsEntityPredicate(), CombinedPredicates.Combine.OR);
        rawPredicate = new RawGraphElementWithStatisticsPredicate(predicate3);
        baos = new ByteArrayOutputStream();
        out = new DataOutputStream(baos);
        rawPredicate.write(out);
        bais = new ByteArrayInputStream(baos.toByteArray());
        in = new DataInputStream(bais);
        read = new RawGraphElementWithStatisticsPredicate();
        read.readFields(in);
        assertEquals(read, rawPredicate);
    }

    @Test
    public void testEquals() {
        // Simple RawGraphElementWithStatisticsPredicate
        Predicate<GraphElementWithStatistics> predicate = new TimeWindowPredicate(new Date(0L), new Date(100L));
        RawGraphElementWithStatisticsPredicate rawPredicate = new RawGraphElementWithStatisticsPredicate(predicate);
        Predicate<GraphElementWithStatistics> predicate2 = new TimeWindowPredicate(new Date(0L), new Date(100L));
        RawGraphElementWithStatisticsPredicate rawPredicate2 = new RawGraphElementWithStatisticsPredicate(predicate2);
        assertEquals(rawPredicate, rawPredicate2);
        assertEquals(rawPredicate.hashCode(), rawPredicate2.hashCode());

        // Composite RawGraphElementWithStatisticsPredicate
        SummaryTypePredicate summaryTypePredicate = new SummaryTypeInSetPredicate(Collections.singleton("abc"));
        Predicate<GraphElementWithStatistics> combined1 = new CombinedPredicates(predicate, summaryTypePredicate, CombinedPredicates.Combine.OR);
        RawGraphElementWithStatisticsPredicate rawPredicate3 = new RawGraphElementWithStatisticsPredicate(combined1);
        SummaryTypePredicate summaryTypePredicate2 = new SummaryTypeInSetPredicate(Collections.singleton("abc"));
        Predicate<GraphElementWithStatistics> combined2 = new CombinedPredicates(predicate2, summaryTypePredicate2, CombinedPredicates.Combine.OR);
        RawGraphElementWithStatisticsPredicate rawPredicate4 = new RawGraphElementWithStatisticsPredicate(combined2);
        assertEquals(rawPredicate3, rawPredicate4);
        assertEquals(rawPredicate.hashCode(), rawPredicate2.hashCode());

        // Another composite RawGraphElementWithStatisticsPredicate
        Predicate<GraphElementWithStatistics> combined3 = new CombinedPredicates(combined1, new IsEntityPredicate(), CombinedPredicates.Combine.OR);
        RawGraphElementWithStatisticsPredicate rawPredicate5 = new RawGraphElementWithStatisticsPredicate(combined3);
        Predicate<GraphElementWithStatistics> combined4 = new CombinedPredicates(combined2, new IsEntityPredicate(), CombinedPredicates.Combine.OR);
        RawGraphElementWithStatisticsPredicate rawPredicate6 = new RawGraphElementWithStatisticsPredicate(combined4);
        assertEquals(rawPredicate5, rawPredicate6);
        assertEquals(rawPredicate5.hashCode(), rawPredicate6.hashCode());
        assertNotEquals(rawPredicate, rawPredicate3);
        assertNotEquals(rawPredicate, rawPredicate5);
    }

    @Test
    public void testAccept() throws IOException {
        // Simple RawGraphElementWithStatisticsPredicate - create predicate, create raw predicate from it, create
        // 2 GEWS, check that one accepts it and one doesn't, create raw, check that raw predicate accepts/rejects
        // in the same way the other one doesn't.
        Predicate<GraphElementWithStatistics> predicate = new TimeWindowPredicate(new Date(0L), new Date(100L));
        RawGraphElementWithStatisticsPredicate rawPredicate = new RawGraphElementWithStatisticsPredicate(predicate);
        GraphElementWithStatistics gews1 = new GraphElementWithStatistics(new GraphElement(
                new Entity("type", "value", "summaryType", "summarySubType", "", new Date(10L), new Date(90L))),
                new SetOfStatistics("count", new Count(1)));
        assertTrue(predicate.accept(gews1));
        RawGraphElementWithStatistics rawGews1 = new RawGraphElementWithStatistics(ConversionUtils.getKeysFromGraphElement(gews1.getGraphElement()).getFirst(),
                ConversionUtils.getValueFromSetOfStatistics(gews1.getSetOfStatistics()));
        assertTrue(rawPredicate.accept(rawGews1));
        GraphElementWithStatistics gews2 = new GraphElementWithStatistics(new GraphElement(
                new Entity("type", "value", "summaryType", "summarySubType", "", new Date(1000L), new Date(10000L))),
                new SetOfStatistics("count", new Count(1)));
        assertFalse(predicate.accept(gews2));
        RawGraphElementWithStatistics rawGews2 = new RawGraphElementWithStatistics(ConversionUtils.getKeysFromGraphElement(gews2.getGraphElement()).getFirst(),
                ConversionUtils.getValueFromSetOfStatistics(gews2.getSetOfStatistics()));
        assertFalse(rawPredicate.accept(rawGews2));

        // Composite RawGraphElementWithStatisticsPredicate
        SummaryTypePredicate summaryTypePredicate = new SummaryTypeInSetPredicate(Collections.singleton("abc"));
        Predicate<GraphElementWithStatistics> predicate2 = new CombinedPredicates(predicate, summaryTypePredicate, CombinedPredicates.Combine.OR);
        rawPredicate = new RawGraphElementWithStatisticsPredicate(predicate2);
        GraphElementWithStatistics gews3 = new GraphElementWithStatistics(new GraphElement(
                new Entity("type", "value", "abc", "summarySubType", "", new Date(10L), new Date(90L))),
                new SetOfStatistics("count", new Count(1)));
        assertTrue(predicate2.accept(gews1));
        assertTrue(predicate2.accept(gews3));
        rawGews1 = new RawGraphElementWithStatistics(ConversionUtils.getKeysFromGraphElement(gews3.getGraphElement()).getFirst(),
                ConversionUtils.getValueFromSetOfStatistics(gews3.getSetOfStatistics()));
        assertTrue(rawPredicate.accept(rawGews1));
        GraphElementWithStatistics gews4 = new GraphElementWithStatistics(new GraphElement(
                new Entity("type", "value", "summaryType", "summarySubType", "", new Date(1000L), new Date(10000L))),
                new SetOfStatistics("count", new Count(1)));
        assertFalse(predicate.accept(gews4));
        RawGraphElementWithStatistics rawGews4 = new RawGraphElementWithStatistics(ConversionUtils.getKeysFromGraphElement(gews4.getGraphElement()).getFirst(),
                ConversionUtils.getValueFromSetOfStatistics(gews4.getSetOfStatistics()));
        assertFalse(rawPredicate.accept(rawGews4));
    }

}

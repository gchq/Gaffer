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
package gaffer.predicate;

import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.summarytype.SummaryTypePredicate;
import gaffer.predicate.summarytype.impl.SummaryTypeInSetPredicate;
import gaffer.predicate.time.impl.AfterTimePredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.junit.Test;

import java.io.*;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test of {@link CombinedPredicates}.
 */
public class TestCombinedPredicates {

    @Test
    public void testAccept() throws IOException {
        AfterTimePredicate predicate1 = new AfterTimePredicate(new Date(100L));
        SummaryTypePredicate predicate2 = new SummaryTypeInSetPredicate("A", "B");
        GraphElement entity = new GraphElement(new Entity("type", "value", "A", "summarySubtype", "public",
                new Date(1000L), new Date(1100L)));
        SetOfStatistics statistics = new SetOfStatistics("count", new Count(1));
        GraphElementWithStatistics gews = new GraphElementWithStatistics(entity, statistics);

        // Test AND
        CombinedPredicates<GraphElementWithStatistics> combined =
                new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.AND);
        assertTrue(combined.accept(gews));
        entity = new GraphElement(new Entity("type", "value", "A", "summarySubtype", "public",
                new Date(10L), new Date(20L)));
        gews = new GraphElementWithStatistics(entity, statistics);
        assertFalse(combined.accept(gews));

        // Test OR
        combined = new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.OR);
        assertTrue(combined.accept(gews));
        entity = new GraphElement(new Entity("type", "value", "A", "summarySubtype", "public",
                new Date(10L), new Date(20L)));
        gews = new GraphElementWithStatistics(entity, statistics);
        assertTrue(combined.accept(gews));
        entity = new GraphElement(new Entity("type", "value", "summaryType", "summarySubtype", "public",
                new Date(10L), new Date(20L)));
        assertTrue(combined.accept(gews));

        // Test XOR
        combined = new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.XOR);
        entity = new GraphElement(new Entity("type", "value", "A", "summarySubtype", "public",
                new Date(10L), new Date(20L)));
        gews = new GraphElementWithStatistics(entity, statistics);
        assertTrue(combined.accept(gews));
        entity = new GraphElement(new Entity("type", "value", "A", "summarySubtype", "public",
                new Date(1000L), new Date(2000L)));
        gews = new GraphElementWithStatistics(entity, statistics);
        assertFalse(combined.accept(gews));
        entity = new GraphElement(new Entity("type", "value", "summaryType", "summarySubtype", "public",
                new Date(1000L), new Date(2000L)));
        gews = new GraphElementWithStatistics(entity, statistics);
        assertTrue(combined.accept(gews));
        entity = new GraphElement(new Entity("type", "value", "summaryType", "summarySubtype", "public",
                new Date(10L), new Date(20L)));
        gews = new GraphElementWithStatistics(entity, statistics);
        assertFalse(combined.accept(gews));
    }

    @Test
    public void testWriteRead() throws IOException {
        AfterTimePredicate predicate1 = new AfterTimePredicate(new Date(100L));
        SummaryTypePredicate predicate2 = new SummaryTypeInSetPredicate("A", "B");

        // Test AND
        CombinedPredicates<GraphElementWithStatistics> combined =
                new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.AND);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        combined.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        CombinedPredicates read = new CombinedPredicates();
        read.readFields(in);
        assertEquals(combined, read);

        // Test OR
        combined = new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.OR);
        baos = new ByteArrayOutputStream();
        out = new DataOutputStream(baos);
        combined.write(out);
        bais = new ByteArrayInputStream(baos.toByteArray());
        in = new DataInputStream(bais);
        read = new CombinedPredicates();
        read.readFields(in);
        assertEquals(combined, read);

        // Test XOR
        combined = new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.XOR);
        baos = new ByteArrayOutputStream();
        out = new DataOutputStream(baos);
        combined.write(out);
        bais = new ByteArrayInputStream(baos.toByteArray());
        in = new DataInputStream(bais);
        read = new CombinedPredicates();
        read.readFields(in);
        assertEquals(combined, read);
    }

    @Test
    public void testEqualsAndHashcode() {
        AfterTimePredicate predicate1 = new AfterTimePredicate(new Date(100L));
        AfterTimePredicate predicate1Copy = new AfterTimePredicate(new Date(100L));
        SummaryTypePredicate predicate2 = new SummaryTypeInSetPredicate("A", "B");
        SummaryTypePredicate predicate2Copy = new SummaryTypeInSetPredicate("A", "B");

        // Test AND
        CombinedPredicates<GraphElementWithStatistics> combined =
                new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.AND);
        CombinedPredicates<GraphElementWithStatistics> combinedCopy =
                new CombinedPredicates<GraphElementWithStatistics>(predicate1Copy, predicate2Copy, CombinedPredicates.Combine.AND);
        assertEquals(combined, combinedCopy);
        assertEquals(combined.hashCode(), combinedCopy.hashCode());

        // Test OR
        combined = new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.OR);
        combinedCopy = new CombinedPredicates<GraphElementWithStatistics>(predicate1Copy, predicate2Copy, CombinedPredicates.Combine.OR);
        assertEquals(combined, combinedCopy);
        assertEquals(combined.hashCode(), combinedCopy.hashCode());

        // Test XOR
        combined = new CombinedPredicates<GraphElementWithStatistics>(predicate1, predicate2, CombinedPredicates.Combine.XOR);
        combinedCopy = new CombinedPredicates<GraphElementWithStatistics>(predicate1Copy, predicate2Copy, CombinedPredicates.Combine.XOR);
        assertEquals(combined, combinedCopy);
        assertEquals(combined.hashCode(), combinedCopy.hashCode());
    }
}

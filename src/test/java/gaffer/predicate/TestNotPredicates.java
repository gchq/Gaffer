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
import gaffer.predicate.time.impl.AfterTimePredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.junit.Test;

import java.io.*;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Unit test of {@link NotPredicate}.
 */
public class TestNotPredicates {

    @Test
    public void testAccept() throws IOException {
        AfterTimePredicate predicate = new AfterTimePredicate(new Date(100L));
        GraphElement entity = new GraphElement(new Entity("type", "value", "A", "summarySubtype", "public",
                new Date(1000L), new Date(1100L)));
        SetOfStatistics statistics = new SetOfStatistics("count", new Count(1));
        GraphElementWithStatistics gews = new GraphElementWithStatistics(entity, statistics);
        NotPredicate<GraphElementWithStatistics> notPredicate = new NotPredicate<GraphElementWithStatistics>(predicate);
        assertFalse(notPredicate.accept(gews));
        entity = new GraphElement(new Entity("type", "value", "A", "summarySubtype", "public",
                new Date(10L), new Date(20L)));
        gews = new GraphElementWithStatistics(entity, statistics);
        assertTrue(notPredicate.accept(gews));
    }

    @Test
    public void testWriteRead() throws IOException {
        AfterTimePredicate predicate = new AfterTimePredicate(new Date(100L));
        NotPredicate<GraphElementWithStatistics> notPredicate = new NotPredicate<GraphElementWithStatistics>(predicate);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        notPredicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        NotPredicate read = new NotPredicate();
        read.readFields(in);
        assertEquals(notPredicate, read);
    }

    @Test
    public void testEqualsAndHashcode() {
        AfterTimePredicate predicate = new AfterTimePredicate(new Date(100L));
        AfterTimePredicate predicateCopy = new AfterTimePredicate(new Date(100L));
        NotPredicate<GraphElementWithStatistics> notPredicate = new NotPredicate<GraphElementWithStatistics>(predicate);
        NotPredicate<GraphElementWithStatistics> notPredicateCopy = new NotPredicate<GraphElementWithStatistics>(predicateCopy);
        assertEquals(notPredicate, notPredicateCopy);
        assertEquals(notPredicate.hashCode(), notPredicateCopy.hashCode());
    }
}

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
package gaffer.predicate.graph.impl;

import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.junit.Test;

import java.io.*;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DirectedEdgePredicate}. Contains tests for writing and reading, and
 * accept.
 */
public class TestDirectedEdgePredicate {

    @Test
    public void testWriteRead() throws IOException {
        DirectedEdgePredicate predicate = new DirectedEdgePredicate();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        DirectedEdgePredicate read = new DirectedEdgePredicate();
        read.readFields(in);
        assertEquals(predicate, read);
    }

    @Test
    public void testAccept() throws IOException {
        DirectedEdgePredicate predicate = new DirectedEdgePredicate();
        Entity entity = new Entity("type", "value", "summaryType", "summarySubType", "visibility", new Date(100L), new Date(1000L));
        SetOfStatistics statistics = new SetOfStatistics();
        statistics.addStatistic("stat", new Count(100));
        GraphElementWithStatistics elementWithStatistics = new GraphElementWithStatistics(new GraphElement(entity), statistics);
        assertTrue(predicate.accept(elementWithStatistics));
        Edge edge = new Edge("srcType", "srcValue", "dstType", "dstValue", "summaryType", "summarySubType", true,
                "visibility", new Date(100L), new Date(1000L));
        elementWithStatistics = new GraphElementWithStatistics(new GraphElement(edge), statistics);
        assertTrue(predicate.accept(elementWithStatistics));
        edge = new Edge("srcType", "srcValue", "dstType", "dstValue", "summaryType", "summarySubType", false,
                "visibility", new Date(100L), new Date(1000L));
        elementWithStatistics = new GraphElementWithStatistics(new GraphElement(edge), statistics);
        assertFalse(predicate.accept(elementWithStatistics));
    }

    @Test
    public void testEquals() {
        DirectedEdgePredicate predicate = new DirectedEdgePredicate();
        DirectedEdgePredicate predicate2 = new DirectedEdgePredicate();
        assertEquals(predicate, predicate2);
        assertEquals(predicate.hashCode(), predicate2.hashCode());
    }

}

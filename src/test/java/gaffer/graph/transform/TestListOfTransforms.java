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
package gaffer.graph.transform;

import gaffer.accumulo.TestAccumuloBackedGraphAsQueryable;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.transform.impl.ListOfTransforms;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.junit.Test;

import java.io.*;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Unit test of {@link ListOfTransforms}.
 */
public class TestListOfTransforms {

    @Test
    public void testWriteRead() throws IOException {
        Transform transform1 = new TestAccumuloBackedGraphAsQueryable.SimpleTransform("abc");
        Transform transform2 = new TestAccumuloBackedGraphAsQueryable.SimpleTransformAndFilter("ghi");
        ListOfTransforms list = new ListOfTransforms(transform1, transform2);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        list.write(out);

        DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        ListOfTransforms read = new ListOfTransforms();
        read.readFields(in);

        assertEquals(list, read);
    }

    @Test
    public void testTransform() {
        Transform transform1 = new TestAccumuloBackedGraphAsQueryable.SimpleTransform("abc");
        Transform transform2 = new TestAccumuloBackedGraphAsQueryable.SimpleTransformAndFilter("ghi");
        ListOfTransforms list = new ListOfTransforms(transform1, transform2);

        Entity entity = new Entity("type", "value", "X", "sub", "vis", new Date(100L), new Date(200L));
        SetOfStatistics statistics = new SetOfStatistics("count", new Count(100));
        GraphElementWithStatistics gews = new GraphElementWithStatistics(new GraphElement(entity), statistics);
        assertNull(list.transform(gews));

        Edge edge = new Edge("srcType", "srcValue", "dstType", "dstValue", "X", "sub", true, "vis", new Date(100L), new Date(200L));
        gews = new GraphElementWithStatistics(new GraphElement(edge), statistics);
        GraphElementWithStatistics transformed = list.transform(gews);
        assertNotNull(transformed);
        assertTrue(transformed.isEdge());
        assertEquals("ghi", transformed.getDestinationType());
        assertEquals("ghi", transformed.getDestinationValue());
    }

    @Test
    public void testEquals() {
        Transform transform1 = new TestAccumuloBackedGraphAsQueryable.SimpleTransform("abc");
        Transform transform2 = new TestAccumuloBackedGraphAsQueryable.SimpleTransformAndFilter("ghi");
        ListOfTransforms list = new ListOfTransforms(transform1, transform2);

        Transform transform3 = new TestAccumuloBackedGraphAsQueryable.SimpleTransform("abc");
        Transform transform4 = new TestAccumuloBackedGraphAsQueryable.SimpleTransformAndFilter("ghi");
        ListOfTransforms list2 = new ListOfTransforms(transform3, transform4);

        assertEquals(list, list2);

        list2 = new ListOfTransforms(transform2, transform3);

        assertNotEquals(list, list2);
    }

}

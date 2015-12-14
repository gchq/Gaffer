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
package gaffer.accumulo.predicate.impl;

import gaffer.Pair;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.predicate.typevalue.impl.TypeInSetPredicate;
import gaffer.predicate.typevalue.impl.ValueRegularExpressionPredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import java.io.*;
import java.util.Date;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test of {@link OtherEndOfEdgePredicate}. Tests that the <code>accept()</code>, <code>write()</code>
 * and <code>readFields()</code>, and <code>equals()</code> methods work correctly.
 */
public class TestOtherEndOfEdgePredicate {

    @Test
    public void testWriteRead() throws IOException {
        OtherEndOfEdgePredicate predicate = new OtherEndOfEdgePredicate(new TypeInSetPredicate("A", "B"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        predicate.write(out);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        OtherEndOfEdgePredicate read = new OtherEndOfEdgePredicate();
        read.readFields(in);
        assertEquals(predicate, read);

        predicate = new OtherEndOfEdgePredicate(new ValueRegularExpressionPredicate(Pattern.compile("A.")));
        baos = new ByteArrayOutputStream();
        out = new DataOutputStream(baos);
        predicate.write(out);
        bais = new ByteArrayInputStream(baos.toByteArray());
        in = new DataInputStream(bais);
        read.readFields(in);
        assertEquals(predicate, read);
    }

    @Test
    public void testAccept() throws IOException {
        OtherEndOfEdgePredicate predicate = new OtherEndOfEdgePredicate(new TypeInSetPredicate("A", "B"));
        SetOfStatistics statistics = new SetOfStatistics("count", new Count(10));
        Value value = ConversionUtils.getValueFromSetOfStatistics(statistics);

        // Should accept Entity
        Entity entity = new Entity("type", "value", "summaryType", "summarySubType", "", new Date(0L), new Date(100L));
        Pair<Key> pair = ConversionUtils.getKeysFromGraphElement(new GraphElement(entity));
        assertTrue(predicate.accept(new RawGraphElementWithStatistics(pair.getFirst(), value)));

        // Should accept only the first copy of this directed Edge
        Edge edge = new Edge("A", "B", "C", "D", "s", "ss", true, "", new Date(0L), new Date(100L));
        pair = ConversionUtils.getKeysFromGraphElement(new GraphElement(edge));
        assertFalse(predicate.accept(new RawGraphElementWithStatistics(pair.getFirst(), value)));
        assertTrue(predicate.accept(new RawGraphElementWithStatistics(pair.getSecond(), value)));

        // Should accept only the second copy of this directed Edge
        edge = new Edge("C", "D", "A", "B", "s", "ss", false, "", new Date(0L), new Date(100L));
        pair = ConversionUtils.getKeysFromGraphElement(new GraphElement(edge));
        assertFalse(predicate.accept(new RawGraphElementWithStatistics(pair.getFirst(), value)));
        assertTrue(predicate.accept(new RawGraphElementWithStatistics(pair.getSecond(), value)));
    }

    @Test
    public void testEquals() {
        OtherEndOfEdgePredicate predicate = new OtherEndOfEdgePredicate(new TypeInSetPredicate("A", "B"));
        OtherEndOfEdgePredicate predicate2 = new OtherEndOfEdgePredicate(new TypeInSetPredicate("A", "B"));
        assertEquals(predicate, predicate2);
        assertEquals(predicate.hashCode(), predicate2.hashCode());
    }
}

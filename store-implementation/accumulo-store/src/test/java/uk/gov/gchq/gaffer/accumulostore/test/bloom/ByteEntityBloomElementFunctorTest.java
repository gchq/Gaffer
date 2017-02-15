/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.accumulostore.test.bloom;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.CoreKeyBloomFunctor;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ByteEntityBloomElementFunctorTest {

    private AccumuloElementConverter elementConverter;
    private Schema schema;

    private final static CoreKeyBloomFunctor elementFunctor = new CoreKeyBloomFunctor();

    @Before
    public void setup() {
        schema = new Schema.Builder()
                .vertexSerialiser(new JavaSerialiser())
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .build();
        elementConverter = new ByteEntityAccumuloElementConverter(schema);

    }

    @Test
    public void shouldTransformRangeEntity() throws AccumuloElementConversionException {
        // Create Range formed from one entity and shouldRetieveElementsInRangeBetweenSeeds
        final Entity entity1 = new Entity(TestGroups.ENTITY);
        entity1.setVertex(1);
        final Key key1 = elementConverter.getKeyFromEntity(entity1);
        final Range range1 = new Range(key1, true, key1, true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(Arrays.copyOf(key1.getRowData().getBackingArray(), key1.getRowData().getBackingArray().length - 2));
        assertTrue(elementFunctor.transform(range1).equals(expectedBloomKey1));

        // Create Range formed from two entities and shouldRetieveElementsInRangeBetweenSeeds - should get null
        final Entity entity2 = new Entity(TestGroups.ENTITY);
        entity2.setVertex(2);
        final Key key2 = elementConverter.getKeyFromEntity(entity2);
        final Range range2 = new Range(key1, true, key2, true);
        assertNull(elementFunctor.transform(range2));
    }

    @Test
    public void shouldTransformKeyEntity() throws AccumuloElementConversionException {
        // Create Key formed from entity and shouldRetieveElementsInRangeBetweenSeeds
        final Entity entity1 = new Entity(TestGroups.ENTITY);
        entity1.setVertex(1);
        final Key key1 = elementConverter.getKeyFromEntity(entity1);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(key1.getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, elementFunctor.transform(key1));
    }

    @Test
    public void shouldTransformRangeEdge() throws AccumuloElementConversionException {
        // Create Range formed from one edge and shouldRetieveElementsInRangeBetweenSeeds
        final Edge edge1 = new Edge(TestGroups.EDGE);
        edge1.setSource(1);
        edge1.setDestination(2);
        final Pair<Key> keys = elementConverter.getKeysFromEdge(edge1);
        final Range range1 = new Range(keys.getFirst().getRow(), true, keys.getFirst().getRow(), true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(keys.getFirst().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, elementFunctor.transform(range1));

        final Range range2 = new Range(keys.getSecond().getRow(), true, keys.getSecond().getRow(), true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey2 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(keys.getSecond().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey2, elementFunctor.transform(range2));

        // Create Range formed from two keys and shouldRetieveElementsInRangeBetweenSeeds - should get null
        final Range range3 = new Range(keys.getFirst().getRow(), true, keys.getSecond().getRow(), true);
        assertNull(elementFunctor.transform(range3));
    }

    @Test
    public void shouldTransformKeyEdge() throws AccumuloElementConversionException {
        // Create Key formed from edge and shouldRetieveElementsInRangeBetweenSeeds
        final Edge edge1 = new Edge(TestGroups.EDGE);
        edge1.setSource(1);
        edge1.setDestination(2);
        final Pair<Key> keys = elementConverter.getKeysFromEdge(edge1);
        final Range range1 = new Range(keys.getFirst().getRow(), true, keys.getFirst().getRow(), true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(keys.getFirst().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, elementFunctor.transform(range1));
        final Range range2 = new Range(keys.getSecond().getRow(), true, keys.getSecond().getRow(), true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey2 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(keys.getSecond().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey2, elementFunctor.transform(range2));
    }

    @Test
    public void shouldTransformRangeFromEntityToEntityAndSomeEdges() throws AccumuloElementConversionException {
        // Create entity
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex(1);
        //        String key1 = ConversionUtils.getRowKeyFromEntity(entity1);
        final Key key1 = elementConverter.getKeyFromEntity(entity);

        // Create edge from that entity
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setSource(1);
        edge.setDestination(2);
        //        String key2 = ConversionUtils.getRowKeysFromEdge(edge).getFirst();
        final Key key2 = elementConverter.getKeysFromEdge(edge).getFirst();

        // Create range from entity to edge inclusive
        final Range range = new Range(key1.getRow(), true, key2.getRow(), true);

        // Check don't get null Bloom key
        assertNotNull(elementFunctor.transform(range));

        // Check get correct Bloom key
        final org.apache.hadoop.util.bloom.Key expectedBloomKey = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(key1.getRowData().getBackingArray()));
        assertEquals(expectedBloomKey, elementFunctor.transform(range));
    }

    @Test
    public void shouldTransformRangeWhenUsingRangeNotExact() {
        try {
            // Create SimpleEntity
            final Entity simpleEntity = new Entity(TestGroups.ENTITY);
            simpleEntity.setVertex("1");
            final Key key = elementConverter.getKeyFromEntity(simpleEntity);
            final Range range = Range.exact(key.getRow());
            final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(key.getRowData().getBackingArray()));
            assertNotNull(elementFunctor.transform(range));
            assertEquals(expectedBloomKey1, elementFunctor.transform(range));
        } catch (AccumuloElementConversionException e) {
            fail("ConversionException " + e);
        }
    }

    @Test
    public void shouldTransformRangeWhenRangeHasUnspecifiedStartOrEndKey() {
        try {
            // Create Range with unspecified start key and shouldRetieveElementsInRangeBetweenSeeds - should get null
            final Edge edge1 = new Edge(TestGroups.EDGE);
            edge1.setSource("3");
            edge1.setDestination("4");
            final Pair<Key> keys = elementConverter.getKeysFromEdge(edge1);
            final Range range1 = new Range(null, true, keys.getFirst().getRow(), true);
            assertNull(elementFunctor.transform(range1));

            // Create Range with unspecified end key and shouldRetieveElementsInRangeBetweenSeeds - should get null
            final Range range2 = new Range(keys.getFirst().getRow(), true, null, true);
            assertNull(elementFunctor.transform(range2));
        } catch (AccumuloElementConversionException e) {
            fail("ConversionException " + e);
        }
    }

    @Test
    public void shouldTransformRangeWhenKeyIsNotEntityOrEdge() {
        // Create arbitrary range
        final Range range = new Range("Blah", true, "MoreBlah", true);
        assertNull(elementFunctor.transform(range));
    }

}

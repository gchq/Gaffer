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

package gaffer.accumulostore.test.bloom;

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.core.impl.CoreKeyBloomFunctor;
import gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Entity;
import gaffer.serialisation.implementation.JavaSerialiser;
import gaffer.store.schema.StoreElementDefinition;
import gaffer.store.schema.StoreSchema;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Gaffer1BloomElementFunctorTest {

    private final static AccumuloElementConverter elementConverter;
    private final static StoreSchema schema;

    static {
        schema = new StoreSchema.Builder()
                .vertexSerialiser(new JavaSerialiser())
                .edge(TestGroups.EDGE, new StoreElementDefinition())
                .entity(TestGroups.ENTITY, new StoreElementDefinition())
                .build();
        elementConverter = new ClassicAccumuloElementConverter(schema);
    }

    private final static CoreKeyBloomFunctor elementFunctor = new CoreKeyBloomFunctor();

    @Test
    public void testTransformRangeEntity() throws AccumuloElementConversionException {
        // Create Range formed from one entity and test
        Entity entity1 = new Entity(TestGroups.ENTITY);
        entity1.setVertex(1);
        Key key1 = elementConverter.getKeyFromEntity(entity1);
        Range range1 = new Range(key1, true, key1, true);
        org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(Arrays.copyOf(key1.getRowData().getBackingArray(), key1.getRowData().getBackingArray().length));
        assertTrue(elementFunctor.transform(range1).equals(expectedBloomKey1));

        // Create Range formed from two entities and test - should get null
        Entity entity2 = new Entity(TestGroups.ENTITY);
        entity2.setVertex(2);
        Key key2 = elementConverter.getKeyFromEntity(entity2);
        Range range2 = new Range(key1, true, key2, true);
        assertNull(elementFunctor.transform(range2));
    }

    @Test
    public void testTransformKeyEntity() throws AccumuloElementConversionException {
        // Create Key formed from entity and test
        Entity entity1 = new Entity(TestGroups.ENTITY);
        entity1.setVertex(1);
        Key key1 = elementConverter.getKeyFromEntity(entity1);
        org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(key1.getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, elementFunctor.transform(key1));
    }

    @Test
    public void testTransformRangeEdge() throws AccumuloElementConversionException {
        // Create Range formed from one edge and test
        Edge edge1 = new Edge(TestGroups.EDGE);
        edge1.setSource(1);
        edge1.setDestination(2);
        Pair<Key> keys = elementConverter.getKeysFromEdge(edge1);
        Range range1 = new Range(keys.getFirst().getRow(), true, keys.getFirst().getRow(), true);
        org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(keys.getFirst().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, elementFunctor.transform(range1));

        Range range2 = new Range(keys.getSecond().getRow(), true, keys.getSecond().getRow(), true);
        org.apache.hadoop.util.bloom.Key expectedBloomKey2 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(keys.getSecond().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey2, elementFunctor.transform(range2));

        // Create Range formed from two keys and test - should get null
        Range range3 = new Range(keys.getFirst().getRow(), true, keys.getSecond().getRow(), true);
        assertNull(elementFunctor.transform(range3));
    }

    @Test
    public void testTransformKeyEdge() throws AccumuloElementConversionException {
        // Create Key formed from edge and test
        Edge edge1 = new Edge(TestGroups.EDGE);
        edge1.setSource(1);
        edge1.setDestination(2);
        Pair<Key> keys = elementConverter.getKeysFromEdge(edge1);
        Range range1 = new Range(keys.getFirst().getRow(), true, keys.getFirst().getRow(), true);
        org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(keys.getFirst().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, elementFunctor.transform(range1));
        Range range2 = new Range(keys.getSecond().getRow(), true, keys.getSecond().getRow(), true);
        org.apache.hadoop.util.bloom.Key expectedBloomKey2 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(keys.getSecond().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey2, elementFunctor.transform(range2));
    }

    @Test
    public void testTransformRangeFromEntityToEntityAndSomeEdges() throws AccumuloElementConversionException {
        // Create entity
        Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex(1);
        //        String key1 = ConversionUtils.getRowKeyFromEntity(entity1);
        Key key1 = elementConverter.getKeyFromEntity(entity);

        // Create edge from that entity
        Edge edge = new Edge(TestGroups.EDGE);
        edge.setSource(1);
        edge.setDestination(2);
        //        String key2 = ConversionUtils.getRowKeysFromEdge(edge).getFirst();
        Key key2 = elementConverter.getKeysFromEdge(edge).getFirst();

        // Create range from entity to edge inclusive
        Range range = new Range(key1.getRow(), true, key2.getRow(), true);

        // Check don't get null Bloom key
        assertNotNull(elementFunctor.transform(range));

        // Check get correct Bloom key
        org.apache.hadoop.util.bloom.Key expectedBloomKey = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(key1.getRowData().getBackingArray()));
        assertEquals(expectedBloomKey, elementFunctor.transform(range));
    }

    @Test
    public void testTransformRangeWhenUsingRangeNotExact() {
        try {
            // Create SimpleEntity
            Entity simpleEntity = new Entity(TestGroups.ENTITY);
            simpleEntity.setVertex("1");
            Key key = elementConverter.getKeyFromEntity(simpleEntity);
            Range range = Range.exact(key.getRow());
            org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(elementFunctor.getVertexFromRangeKey(key.getRowData().getBackingArray()));
            assertNotNull(elementFunctor.transform(range));
            assertEquals(expectedBloomKey1, elementFunctor.transform(range));
        } catch (AccumuloElementConversionException e) {
            fail("ConversionException " + e);
        }
    }

    @Test
    public void testTransformRangeWhenRangeHasUnspecifiedStartOrEndKey() {
        try {
            // Create Range with unspecified start key and test - should get null
            Edge edge1 = new Edge(TestGroups.EDGE);
            edge1.setSource("3");
            edge1.setDestination("4");
            Pair<Key> keys = elementConverter.getKeysFromEdge(edge1);
            Range range1 = new Range(null, true, keys.getFirst().getRow(), true);
            assertNull(elementFunctor.transform(range1));

            // Create Range with unspecified end key and test - should get null
            Range range2 = new Range(keys.getFirst().getRow(), true, null, true);
            assertNull(elementFunctor.transform(range2));
        } catch (AccumuloElementConversionException e) {
            fail("ConversionException " + e);
        }
    }

    @Test
    public void testTransformRangeWhenKeyIsNotEntityOrEdge() {
        // Create arbitrary range
        Range range = new Range("Blah", true, "MoreBlah", true);
        assertNull(elementFunctor.transform(range));
    }

}

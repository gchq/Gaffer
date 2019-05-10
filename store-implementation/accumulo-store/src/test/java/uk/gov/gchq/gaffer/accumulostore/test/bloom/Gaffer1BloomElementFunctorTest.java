/*
 * Copyright 2016-2019 Crown Copyright
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
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Gaffer1BloomElementFunctorTest {

    private AccumuloElementConverter elementConverter;
    private Schema schema;

    private static final CoreKeyBloomFunctor ELEMENT_FUNCTOR = new CoreKeyBloomFunctor();

    @Before
    public void setup() {
        schema = new Schema.Builder()
                .vertexSerialiser(new JavaSerialiser())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition())
                .build();
        elementConverter = new ClassicAccumuloElementConverter(schema);
    }

    @Test
    public void shouldTransformRangeEntity() {
        // Create Range formed from one entity and shouldRetrieveElementsInRangeBetweenSeeds
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(1)
                .build();
        final Key key1 = elementConverter.getKeyFromEntity(entity1);
        final Range range1 = new Range(key1, true, key1, true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(Arrays.copyOf(key1.getRowData().getBackingArray(), key1.getRowData().getBackingArray().length));
        assertTrue(ELEMENT_FUNCTOR.transform(range1).equals(expectedBloomKey1));

        // Create Range formed from two entities and shouldRetrieveElementsInRangeBetweenSeeds - should get null
        final Entity entity2 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(2)
                .build();
        final Key key2 = elementConverter.getKeyFromEntity(entity2);
        final Range range2 = new Range(key1, true, key2, true);
        assertNull(ELEMENT_FUNCTOR.transform(range2));
    }

    @Test
    public void shouldTransformKeyEntity() {
        // Create Key formed from entity and shouldRetrieveElementsInRangeBetweenSeeds
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(1)
                .build();
        final Key key1 = elementConverter.getKeyFromEntity(entity1);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ELEMENT_FUNCTOR.getVertexFromRangeKey(key1.getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, ELEMENT_FUNCTOR.transform(key1));
    }

    @Test
    public void shouldTransformRangeEdge() {
        // Create Range formed from one edge and shouldRetrieveElementsInRangeBetweenSeeds
        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(1)
                .dest(2).build();
        final Pair<Key, Key> keys = elementConverter.getKeysFromEdge(edge1);
        final Range range1 = new Range(keys.getFirst().getRow(), true, keys.getFirst().getRow(), true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ELEMENT_FUNCTOR.getVertexFromRangeKey(keys.getFirst().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, ELEMENT_FUNCTOR.transform(range1));

        final Range range2 = new Range(keys.getSecond().getRow(), true, keys.getSecond().getRow(), true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey2 = new org.apache.hadoop.util.bloom.Key(ELEMENT_FUNCTOR.getVertexFromRangeKey(keys.getSecond().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey2, ELEMENT_FUNCTOR.transform(range2));

        // Create Range formed from two keys and shouldRetrieveElementsInRangeBetweenSeeds - should get null
        final Range range3 = new Range(keys.getFirst().getRow(), true, keys.getSecond().getRow(), true);
        assertNull(ELEMENT_FUNCTOR.transform(range3));
    }

    @Test
    public void shouldTransformKeyEdge() {
        // Create Key formed from edge and shouldRetrieveElementsInRangeBetweenSeeds
        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(1)
                .dest(2).build();
        final Pair<Key, Key> keys = elementConverter.getKeysFromEdge(edge1);
        final Range range1 = new Range(keys.getFirst().getRow(), true, keys.getFirst().getRow(), true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ELEMENT_FUNCTOR.getVertexFromRangeKey(keys.getFirst().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey1, ELEMENT_FUNCTOR.transform(range1));
        final Range range2 = new Range(keys.getSecond().getRow(), true, keys.getSecond().getRow(), true);
        final org.apache.hadoop.util.bloom.Key expectedBloomKey2 = new org.apache.hadoop.util.bloom.Key(ELEMENT_FUNCTOR.getVertexFromRangeKey(keys.getSecond().getRowData().getBackingArray()));
        assertEquals(expectedBloomKey2, ELEMENT_FUNCTOR.transform(range2));
    }

    @Test
    public void shouldTransformRangeFromEntityToEntityAndSomeEdges() {
        // Create entity
        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(1).build();
        //        String key1 = ConversionUtils.getRowKeyFromEntity(entity1);
        final Key key1 = elementConverter.getKeyFromEntity(entity);

        // Create edge from that entity
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(1)
                .dest(2).build();
        //        String key2 = ConversionUtils.getRowKeysFromEdge(edge).getFirst();
        final Key key2 = elementConverter.getKeysFromEdge(edge).getFirst();

        // Create range from entity to edge inclusive
        final Range range = new Range(key1.getRow(), true, key2.getRow(), true);

        // Check don't get null Bloom key
        assertNotNull(ELEMENT_FUNCTOR.transform(range));

        // Check get correct Bloom key
        final org.apache.hadoop.util.bloom.Key expectedBloomKey = new org.apache.hadoop.util.bloom.Key(ELEMENT_FUNCTOR.getVertexFromRangeKey(key1.getRowData().getBackingArray()));
        assertEquals(expectedBloomKey, ELEMENT_FUNCTOR.transform(range));
    }

    @Test
    public void shouldTransformRangeWhenUsingRangeNotExact() {
        try {
            // Create SimpleEntity
            final Entity simpleEntity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("1")
                    .build();
            final Key key = elementConverter.getKeyFromEntity(simpleEntity);
            final Range range = Range.exact(key.getRow());
            final org.apache.hadoop.util.bloom.Key expectedBloomKey1 = new org.apache.hadoop.util.bloom.Key(ELEMENT_FUNCTOR.getVertexFromRangeKey(key.getRowData().getBackingArray()));
            assertNotNull(ELEMENT_FUNCTOR.transform(range));
            assertEquals(expectedBloomKey1, ELEMENT_FUNCTOR.transform(range));
        } catch (final AccumuloElementConversionException e) {
            fail("ConversionException " + e);
        }
    }

    @Test
    public void shouldTransformRangeWhenRangeHasUnspecifiedStartOrEndKey() {
        try {
            // Create Range with unspecified start key and shouldRetrieveElementsInRangeBetweenSeeds - should get null
            final Edge edge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("3")
                    .dest("4").build();
            final Pair<Key, Key> keys = elementConverter.getKeysFromEdge(edge1);
            final Range range1 = new Range(null, true, keys.getFirst().getRow(), true);
            assertNull(ELEMENT_FUNCTOR.transform(range1));

            // Create Range with unspecified end key and shouldRetrieveElementsInRangeBetweenSeeds - should get null
            final Range range2 = new Range(keys.getFirst().getRow(), true, null, true);
            assertNull(ELEMENT_FUNCTOR.transform(range2));
        } catch (final AccumuloElementConversionException e) {
            fail("ConversionException " + e);
        }
    }

    @Test
    public void shouldTransformRangeWhenKeyIsNotEntityOrEdge() {
        // Create arbitrary range
        final Range range = new Range("Blah", true, "MoreBlah", true);
        assertNull(ELEMENT_FUNCTOR.transform(range));
    }

}

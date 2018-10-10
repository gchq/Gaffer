/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.join.merge;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElementMergeTest {
    final Entity testEntity = getEntity(3);
    final Entity testEntity1 = getEntity(5);
    final Entity testEntity2 = getEntity(7);
    final Entity testEntity3 = getEntity(9);

    final List inputList = Arrays.asList(ImmutableMap.of(testEntity, Arrays.asList(testEntity1, testEntity2)),
            ImmutableMap.of(testEntity, Arrays.asList(testEntity1, testEntity2)),
            ImmutableMap.of(testEntity2, Arrays.asList(testEntity2, testEntity3)));

    final Schema schemaNoAggregation = new Schema.Builder()
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex(TestTypes.ID_STRING)
                    .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                    .property(TestPropertyNames.SET, TestTypes.PROP_SET_STRING)
                    .aggregate(false)
                    .build())
            .build();

    final Schema schemaWithAggregation = new Schema.Builder()
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex(TestTypes.ID_STRING)
                    .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                    .property(TestPropertyNames.SET, TestTypes.PROP_SET_STRING)
                    .aggregator(new ElementAggregator.Builder().select("count").execute(new Sum()).build())
                    .build())
            .build();

    @Test
    public void shouldFlattenGettingKeys() throws OperationException {
        // Given
        final List<Element> expectedMergedElements = Arrays.asList(testEntity, testEntity, testEntity2);

        final ElementMerge merger = new ElementMerge(ResultsWanted.KEY_ONLY, MergeType.NONE);
        merger.setSchema(schemaNoAggregation);

        // When
        final List mergeResults = merger.merge(inputList);

        // Then
        assertEquals(expectedMergedElements.size(), mergeResults.size());
        assertTrue(mergeResults.containsAll(expectedMergedElements));
    }

    @Test
    public void shouldFlattenGettingRelatedOnly() throws OperationException {
        // Given
        final List<Element> expectedMergedElements = Arrays.asList(testEntity1, testEntity2, testEntity1, testEntity2, testEntity2, testEntity3);

        final ElementMerge merger = new ElementMerge(ResultsWanted.RELATED_ONLY, MergeType.NONE);
        merger.setSchema(schemaNoAggregation);

        // When
        final List mergeResults = merger.merge(inputList);

        // Then
        assertEquals(expectedMergedElements.size(), mergeResults.size());
        assertTrue(mergeResults.containsAll(expectedMergedElements));
    }

    @Test
    public void shouldFlattenGettingBoth() throws OperationException {
        // Given
        final List<Element> expectedMergedElements = Arrays.asList(testEntity, testEntity, testEntity, testEntity1, testEntity2, testEntity2, testEntity2, testEntity2, testEntity3);

        final ElementMerge merger = new ElementMerge(ResultsWanted.BOTH, MergeType.NONE);
        merger.setSchema(schemaNoAggregation);

        // When
        final List mergeResults = merger.merge(inputList);

        // Then
        assertEquals(expectedMergedElements.size(), mergeResults.size());
        assertTrue(mergeResults.containsAll(expectedMergedElements));
    }

    @Test
    public void shouldMergeAgainstKeyGettingRelatedElements() throws OperationException {
        // Given
        final List<Element> expectedMergedElements = Arrays.asList(getEntity(12), getEntity(12), getEntity(16));

        final ElementMerge merger = new ElementMerge(ResultsWanted.RELATED_ONLY, MergeType.RELATED_ONLY);
        merger.setSchema(schemaWithAggregation);

        // When
        final List mergeResults = merger.merge(inputList);

        // Then
        assertEquals(expectedMergedElements.size(), mergeResults.size());
        assertTrue(mergeResults.containsAll(expectedMergedElements));
    }

    @Test
    public void shouldMergeAgainstKeyGettingBoth() throws OperationException {
        // Given
        final List<Element> expectedMergedElements = Arrays.asList(testEntity, getEntity(12), testEntity, getEntity(12), testEntity2, getEntity(16));

        final ElementMerge merger = new ElementMerge(ResultsWanted.BOTH, MergeType.RELATED_ONLY);
        merger.setSchema(schemaWithAggregation);

        // When
        final List mergeResults = merger.merge(inputList);

        // Then
        assertEquals(expectedMergedElements.size(), mergeResults.size());
        assertTrue(mergeResults.containsAll(expectedMergedElements));
    }

    @Test
    public void shouldMergeAgainstKeyGettingKeys() throws OperationException {
        // Given
        final List<Element> expectedMergedElements = Arrays.asList(testEntity, testEntity, testEntity2);

        final ElementMerge merger = new ElementMerge(ResultsWanted.KEY_ONLY, MergeType.RELATED_ONLY);
        merger.setSchema(schemaWithAggregation);

        // When
        final List mergeResults = merger.merge(inputList);

        // Then
        assertEquals(expectedMergedElements.size(), mergeResults.size());
        assertTrue(mergeResults.containsAll(expectedMergedElements));
    }

    @Test
    public void shouldMergeBothGettingAll() throws OperationException {
        // Then
        final List<Element> expectedMergedElements = Arrays.asList(getEntity(15), getEntity(15), getEntity(23));

        final ElementMerge merger = new ElementMerge(ResultsWanted.BOTH, MergeType.BOTH);
        merger.setSchema(schemaWithAggregation);

        // When
        final List mergeResults = merger.merge(inputList);

        // Then
        assertEquals(expectedMergedElements.size(), mergeResults.size());
        assertTrue(mergeResults.containsAll(expectedMergedElements));
    }

    private Entity getEntity(final Integer countProperty) {
        return new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, Long.parseLong(countProperty.toString()))
                .build();
    }
}
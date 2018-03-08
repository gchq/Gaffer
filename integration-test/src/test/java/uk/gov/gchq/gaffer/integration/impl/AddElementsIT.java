/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.integration.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class AddElementsIT extends AbstractStoreIT {

    public static final Entity VALID = new Entity.Builder()
            .group(TestGroups.ENTITY_2)
            .vertex("1")
            .property(TestPropertyNames.TIMESTAMP, Long.MAX_VALUE)
            .property(TestPropertyNames.INT, 1)
            .build();
    public static final Entity INVALID = new Entity.Builder()
            .group(TestGroups.ENTITY_2)
            .vertex("2")
            .property(TestPropertyNames.TIMESTAMP, 1L)
            .property(TestPropertyNames.INT, 21)
            .build();

    @Override
    public void addDefaultElements() throws OperationException {
        // do not add any elements
    }

    @Test
    public void shouldThrowExceptionWithUsefulMessageWhenInvalidElementsAdded() throws OperationException {
        // Given
        final AddElements addElements = new AddElements.Builder()
                .input(VALID, INVALID)
                .build();


        // When / Then
        try {
            graph.execute(addElements, getUser());
        } catch (final Exception e) {
            String msg = e.getMessage();
            if (!msg.contains("Element of type Entity") && null != e.getCause()) {
                msg = e.getCause().getMessage();
            }

            assertTrue("Message was: " + msg, msg.contains("IsLessThan"));
            assertTrue("Message was: " + msg, msg.contains("returned false for properties: {intProperty: <java.lang.Integer>21}"));
            assertTrue("Message was: " + msg, msg.contains("AgeOff"));
            assertTrue("Message was: " + msg, msg.contains("returned false for properties: {timestamp: <java.lang.Long>1}"));
        }
    }

    @Test
    public void shouldNotThrowExceptionWhenInvalidElementsAddedWithSkipInvalidSetToTrue() throws OperationException {
        // Given
        final AddElements addElements = new AddElements.Builder()
                .input(VALID, INVALID)
                .skipInvalidElements(true)
                .build();

        // When
        graph.execute(addElements, getUser());

        // Then - no exceptions
    }

    @Test
    public void shouldNotThrowExceptionWhenInvalidElementsAddedWithValidateSetToFalse() throws OperationException {
        // Given
        final AddElements addElements = new AddElements.Builder()
                .input(VALID, INVALID)
                .validate(false)
                .build();

        // When
        graph.execute(addElements, getUser());

        // Then - no exceptions
    }

    @Test
    public void shouldAddElementsWithSameTimestampWithoutAggregation() throws OperationException {
        // Given
        final Graph graphWithNoAggregation = createGraphWithNoAggregation();
        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .property(TestPropertyNames.COUNT, 1L)
                .property(TestPropertyNames.TIMESTAMP, 1L)
                .build();
        final Entity entity2 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .property(TestPropertyNames.COUNT, 2L)
                .property(TestPropertyNames.TIMESTAMP, 1L)
                .build();
        final Entity entity3 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .property(TestPropertyNames.COUNT, 3L)
                .property(TestPropertyNames.TIMESTAMP, 2L)
                .build();

        final AddElements addElements = new AddElements.Builder()
                .input(entity, entity2, entity3)
                .build();

        // When
        graphWithNoAggregation.execute(addElements, getUser());

        // Then
        final CloseableIterable<? extends Element> allElements = graphWithNoAggregation.execute(new GetAllElements(), getUser());
        ElementUtil.assertElementEquals(Arrays.asList(entity, entity2, entity3), allElements);
    }

    @TraitRequirement(StoreTrait.INGEST_AGGREGATION)
    @Test
    public void shouldAddElementsWithSameTimestampWithAggregation() throws OperationException {
        // Given
        final Graph graphWithAggregation = createGraphWithAggregation();
        final Entity entity1 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .property(TestPropertyNames.COUNT, 1L)
                .property(TestPropertyNames.TIMESTAMP, 1L)
                .build();
        final Entity entity2 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .property(TestPropertyNames.COUNT, 2L)
                .property(TestPropertyNames.TIMESTAMP, 1L)
                .build();
        final Entity entity3 = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .property(TestPropertyNames.COUNT, 3L)
                .property(TestPropertyNames.TIMESTAMP, 2L)
                .build();

        final AddElements addElements = new AddElements.Builder()
                .input(entity1, entity2, entity3)
                .build();

        // When
        graphWithAggregation.execute(addElements, getUser());

        // Then
        final CloseableIterable<? extends Element> allElements = graphWithAggregation.execute(new GetAllElements(), getUser());
        ElementUtil.assertElementEquals(Collections.singletonList(
                        new Entity.Builder()
                                .group(TestGroups.ENTITY)
                                .vertex("1")
                                .property(TestPropertyNames.COUNT, 6L)
                                .property(TestPropertyNames.TIMESTAMP, 2L)
                                .build()),
                allElements);
    }

    private Graph createGraphWithAggregation() {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("integrationTestGraphWithAggregation")
                        .build())
                .storeProperties(getStoreProperties())
                .addSchema(createSchemaWithAggregation())
                .addSchema(getStoreSchema())
                .build();
    }

    private Graph createGraphWithNoAggregation() {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("integrationTestGraphWithNoAggregation")
                        .build())
                .storeProperties(getStoreProperties())
                .addSchema(createSchemaWithNoAggregation())
                .addSchema(getStoreSchema())
                .build();
    }

    private Schema createSchemaWithAggregation() {
        return new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .aggregate(true)
                        .build())
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Sum())
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .timestampProperty(TestTypes.TIMESTAMP)
                .build();
    }

    private Schema createSchemaWithNoAggregation() {
        return new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .aggregate(false)
                        .build())
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.TIMESTAMP, Long.class)
                .type(TestTypes.PROP_COUNT, Long.class)
                .timestampProperty(TestTypes.TIMESTAMP)
                .build();
    }
}

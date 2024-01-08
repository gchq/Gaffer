/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.factory;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AbstractExamplesFactoryTest {

    private static final Schema SCHEMA  = new Schema.Builder()
            .json(StreamUtil.schema(TestExamplesFactory.class))
            .build();

    @Test
    void shouldUseSchemaToCreateGetElementsInput() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        GetElements operation = (GetElements) examplesFactory.generateExample(GetElements.class);

        // Then
        int size = 0;
        for (ElementId e : operation.getInput()) {
            size++;
            if (e instanceof EntityId) {
                assertThat(((EntityId) e).getVertex()).isExactlyInstanceOf(String.class);
            } else {
                assertThat(((EdgeId) e).getDestination()).isExactlyInstanceOf(String.class);
                assertThat(((EdgeId) e).getSource()).isExactlyInstanceOf(String.class);
            }
        }
        assertThat(size).isEqualTo(2);
    }

    @Test
    void shouldUseSchemaToCreateGetAdjacentIdsInput() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        GetAdjacentIds operation = (GetAdjacentIds) examplesFactory.generateExample(GetAdjacentIds.class);

        // Then
        int size = 0;
        for (ElementId e : operation.getInput()) {
            size++;
            if (e instanceof EntityId) {
                assertThat(((EntityId) e).getVertex()).isExactlyInstanceOf(String.class);
            } else {
                throw new RuntimeException("Expected operation only to contain entity ids");
            }
        }
        assertThat(size).isEqualTo(1);
    }

    @Test
    void shouldPopulateAddElementsAccordingToSchema() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        AddElements operation = (AddElements) examplesFactory.generateExample(AddElements.class);

        // Then
        List<Element> expectedInput = Arrays.asList(
                new Entity.Builder()
                        .group("BasicEntity")
                        .vertex("vertex1")
                        .property("count", 1)
                        .build(),
                new Entity.Builder()
                        .group("BasicEntity")
                        .vertex("vertex2")
                        .property("count", 2)
                        .build(),
                new Edge.Builder()
                        .group("BasicEdge")
                        .source("vertex1")
                        .dest("vertex2")
                        .directed(true)
                        .property("count", 1)
                        .build()
        );

        assertThat(Lists.newArrayList(operation.getInput())).isEqualTo(expectedInput);
    }

    @Test
    void shouldUseSchemaForGroupsInSortOperation() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        Sort operation = (Sort) examplesFactory.generateExample(Sort.class);

        // Then
        // Sort has no equals method
        assertThat(operation.getComparators().size()).isEqualTo(1);
        assertThat(((ElementPropertyComparator) operation.getComparators().get(0)).getGroups()).containsOnly("BasicEdge");
        assertThat(((ElementPropertyComparator) operation.getComparators().get(0)).getProperty()).isEqualTo("count");
    }

    @Test
    void shouldUseSchemaForMaxOperation() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        Max operation = (Max) examplesFactory.generateExample(Max.class);

        // Then
        // Max has no equals method
        assertThat(operation.getComparators().size()).isEqualTo(1);
        assertThat(((ElementPropertyComparator) operation.getComparators().get(0)).getGroups()).containsOnly("BasicEdge");
        assertThat(((ElementPropertyComparator) operation.getComparators().get(0)).getProperty()).isEqualTo("count");
    }

    @Test
    void shouldUseSchemaForMinOperation() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        Min operation = (Min) examplesFactory.generateExample(Min.class);

        // Then
        // Min has no equals method
        assertThat(operation.getComparators().size()).isEqualTo(1);
        assertThat(((ElementPropertyComparator) operation.getComparators().get(0)).getGroups()).containsOnly("BasicEdge");
        assertThat(((ElementPropertyComparator) operation.getComparators().get(0)).getProperty()).isEqualTo("count");
    }

    @Test
    void shouldProvideEmptyGetWalksIfSchemaEmpty() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(new Schema());

        // When
        GetWalks operation = (GetWalks) examplesFactory.generateExample(GetWalks.class);

        // Then
        assertThat(operation.getInput()).isNull();
        assertThat(operation.getOperations().size()).isEqualTo(0);
    }

    @Test
    void shouldProvideEmptyGetWalksIfSchemaContainsNoEdges() throws InstantiationException, IllegalAccessException {
        // Given
        final Schema schemaNoEdges = new Schema.Builder()
                .json(StreamUtil.openStream(TestExamplesFactory.class, "schema/schemaNoEdges.json"))
                .build();
        final TestExamplesFactory examplesFactory = new TestExamplesFactory(schemaNoEdges);

        // When
        final GetWalks operation = (GetWalks) examplesFactory.generateExample(GetWalks.class);

        // Then
        assertThat(operation.getInput()).isNull();
        assertThat(operation.getOperations()).hasSize(0);
    }

    @Test
    void shouldProvideEmptyGetWalksIfSchemaContainsNoEntities() throws InstantiationException, IllegalAccessException {
        // Given
        final Schema schemaNoEdges = new Schema.Builder()
                .json(StreamUtil.openStream(TestExamplesFactory.class, "schema/schemaNoEntities.json"))
                .build();
        final TestExamplesFactory examplesFactory = new TestExamplesFactory(schemaNoEdges);

        // When
        final GetWalks operation = (GetWalks) examplesFactory.generateExample(GetWalks.class);

        // Then
        assertThat(operation.getInput()).isNull();
        assertThat(operation.getOperations()).hasSize(0);
    }

    @Test
    void shouldProvideSchemaPopulatedGetWalksIfSchemaContainsEdges() throws InstantiationException, IllegalAccessException {
        // Given
        TestExamplesFactory examplesFactory = new TestExamplesFactory(SCHEMA);

        // When
        GetWalks operation = (GetWalks) examplesFactory.generateExample(GetWalks.class);
        List<OperationChain<Iterable<? extends Element>>> expectedOperations = Arrays.asList(
                new OperationChain.Builder()
                        .first(new GetElements.Builder()
                            .view(new View.Builder()
                                .edge("BasicEdge")
                                .build())
                            .build())
                        .build());

        // Then
        assertThat(operation.getInput()).singleElement().isEqualTo(new EntitySeed("vertex1"));
        assertThat(operation.getOperations()).isEqualTo(expectedOperations);
    }

    @Test
    void shouldAssumeEdgesDirectedFieldIfSchemaDoesNotSpecify() throws InstantiationException, IllegalAccessException {
        // Given
        final Schema schemaNoDirected = new Schema.Builder()
                .json(StreamUtil.openStream(TestExamplesFactory.class, "schema/schemaNoDirected.json"))
                .build();
        final TestExamplesFactory examplesFactory = new TestExamplesFactory(schemaNoDirected);

        // When
        GetElements getElementsOperation = (GetElements) examplesFactory.generateExample(GetElements.class);
        AddElements addElementsOperation = (AddElements) examplesFactory.generateExample(AddElements.class);

        // Then
        for (ElementId e : getElementsOperation.getInput()) {
            if (e instanceof EdgeId) {
                assertThat(((EdgeId) e).getDirectedType()).isEqualTo(DirectedType.EITHER);
            }
        }
        for (ElementId e : addElementsOperation.getInput()) {
            if (e instanceof EdgeId) {
                assertThat(((EdgeId) e).getDirectedType()).isEqualTo(DirectedType.UNDIRECTED);
            }
        }
    }

    private static class TestExamplesFactory extends AbstractExamplesFactory {

        private final Schema schema;

        TestExamplesFactory(final Schema schema) {
            this.schema = schema;
        }

        @Override
        protected Schema getSchema() {
            return this.schema;
        }
    }
}

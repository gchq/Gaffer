/*
 * Copyright 2018-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.integration.delete;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Entity.Builder;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.List;

public abstract class AbstractDeletedElementsIT<OP extends Output<O>, O> {
    protected static final String[] VERTICES = {"1", "2", "3"};

    protected abstract OP createGetOperation();

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(currentClass));

    protected void assertElements(final Iterable<ElementId> expected, final O actual) {
        ElementUtil.assertElementEquals(expected, (Iterable) actual);
    }

    @Test
    public void shouldNotReturnDeletedElements() throws Exception {
        // Given
        final AccumuloStore accStore = (AccumuloStore) AccumuloStore.createStore(
                "graph1",
                new Schema.Builder()
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .build())
                        .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                                .source(TestTypes.ID_STRING)
                                .destination(TestTypes.ID_STRING)
                                .directed(TestTypes.DIRECTED_EITHER)
                                .build())
                        .type(TestTypes.ID_STRING, String.class)
                        .type(TestTypes.DIRECTED_EITHER, Boolean.class)
                        .build(),
                PROPERTIES
        );

        final Graph graph = new Graph.Builder()
                .store(accStore)
                .build();

        final Entity entityToDelete = new Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .build();
        final Edge edgeToDelete = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("2")
                .directed(true)
                .build();
        final Entity entityToKeep = new Builder()
                .group(TestGroups.ENTITY)
                .vertex("2")
                .build();
        final Edge edgeToKeep = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("2")
                .dest("3")
                .directed(true)
                .build();
        final List<Element> elements = Arrays.asList(
                entityToDelete,
                entityToKeep,
                edgeToDelete,
                edgeToKeep);

        graph.execute(new AddElements.Builder()
                .input(elements)
                .build(), new User());

        final O resultBefore = graph.execute(createGetOperation(), new User());
        assertElements((Iterable) elements, resultBefore);

        // When
        final OperationChain<Void> chain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("1"))
                        .build())
                .then(new DeleteElements())
                .build();
        graph.execute(chain, new User());

        // Then
        final O resultAfter = graph.execute(createGetOperation(), new User());
        assertElements(Arrays.asList(entityToKeep, edgeToKeep), resultAfter);
    }
}

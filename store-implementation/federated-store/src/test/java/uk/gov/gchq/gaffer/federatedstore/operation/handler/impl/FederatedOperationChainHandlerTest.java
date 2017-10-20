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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.integration.FederatedStoreITs;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;

import java.util.Arrays;

public class FederatedOperationChainHandlerTest {

    private Element[] elements = new Element[]{
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("1")
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("2")
                    .build(),
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .build()
    };;

    @Test
    public void shouldHandleChain() throws OperationException {
        // Given
        final FederatedStore store = createStore();
        final Context context = new Context();

        final FederatedOperationChain<Iterable<? extends Element>> opChain = new FederatedOperationChain<>(
                new OperationChain.Builder()
                        .first(new GetAllElements())
                        .then(new Limit<>(10))
                        .build()
        );

        // When
        final Iterable result = store.execute(opChain, context);

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(elements), result);
    }

    private FederatedStore createStore() throws OperationException {
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .aggregate(false)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .aggregate(false)
                        .build())
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .validateFunctions(new IsTrue())
                        .build())
                .build();
        final FederatedStore store = (FederatedStore) Store.createStore("federatedGraph", schema, StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, "predefinedFederatedStore.properties")));

        final Context context = new Context();

        store.execute(
                new FederatedOperationChain<>(
                        new AddElements.Builder()
                                .input(elements)
                                .build()),
                context
        ); return store;
    }
}
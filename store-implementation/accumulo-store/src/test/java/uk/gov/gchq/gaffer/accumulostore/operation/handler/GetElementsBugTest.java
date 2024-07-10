/*
 * Copyright 2016-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import org.apache.accumulo.core.client.TableExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class GetElementsBugTest {

    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(GetElementsBugTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties
            .loadStoreProperties(StreamUtil.openStream(GetElementsBugTest.class, "/accumuloStore.properties"));
    private static final AccumuloStore GAFFER_1_KEY_STORE = new SingleUseMiniAccumuloStore();

    final Set<EntityId> seeds = new HashSet<>(Arrays.asList(new EntitySeed("A0"), new EntitySeed("A23")));

    private final User user = new User();

    @BeforeEach
    public void reInitialise() throws StoreException, OperationException, TableExistsException {
        GAFFER_1_KEY_STORE.initialise("gaffer", SCHEMA, CLASSIC_PROPERTIES);
        setupGraph(GAFFER_1_KEY_STORE);
    }

    @Test
    public void should() throws OperationException {
        GAFFER_1_KEY_STORE.getProperties().setMaxEntriesForBatchScanner("50");
        final Set<EntityId> seeds = new LinkedHashSet<>();
        for (int i = 0; i < 300 ; i++) {
            seeds.add(new EntitySeed("A" + i));
        }
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .view(new View.Builder()
                    .edge(TestGroups.EDGE)
                .build())
                .input(seeds)
                .build();
        final GetElementsWithinSetHandler handler = new GetElementsWithinSetHandler();

        final Iterable<? extends Element> results = handler.doOperation(op, user, GAFFER_1_KEY_STORE);

        assertThat(results).hasSize(7);
    }

    private static void setupGraph(final AccumuloStore store) throws OperationException, StoreException, TableExistsException {
        // Create table
        // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
        // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
        TableUtils.createTable(store);

        final Set<Element> data = new HashSet<>();
        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        data.add(entity);
        for (int i = 0; i < 300; i++) {
            data.add(new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("A" + i)
                    .property(AccumuloPropertyNames.COUNT, i)
                    .build());
        }
        // This edge doesn't get returned as it is the source is in the first batch and the
        // dest is in the final batch
        data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A68")
                    .dest("A281")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build());
        data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A1") //range 1
                    .dest("A52") // range 2
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build());
        data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A150")
                    .dest("A299")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build());
        data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A244")
                    .dest("A87")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build());
        data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A168")
                    .dest("A130")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build());
        data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A297")
                    .dest("A193")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build());
        data.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A125")
                    .dest("A63")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build());
        addElements(data, new User(), store);
    }

    private static void addElements(final Iterable<Element> data, final User user, final AccumuloStore store) throws OperationException {
        store.execute(new AddElements.Builder().input(data).build(), new Context(user));
    }
}

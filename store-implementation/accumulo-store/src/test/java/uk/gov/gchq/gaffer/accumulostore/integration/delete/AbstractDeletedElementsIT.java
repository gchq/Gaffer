/*
 * Copyright 2017-2018 Crown Copyright
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

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Entity.Builder;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

public abstract class AbstractDeletedElementsIT<OP extends Output<O>, O> {
    protected static final String[] VERTICES = {"1", "2", "3"};

    protected abstract OP createGetOperation();

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
                AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(getClass()))
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
        deleteElement(entityToDelete, accStore);
        deleteElement(edgeToDelete, accStore);

        // Then
        final O resultAfter = graph.execute(createGetOperation(), new User());
        assertElements(Arrays.asList(entityToKeep, edgeToKeep), resultAfter);
    }

    private void deleteElement(final Element element, AccumuloStore accStore) throws Exception {
        final AccumuloElementConverter converter = accStore.getKeyPackage().getKeyConverter();
        final Pair<Key, Key> keys = converter.getKeysFromElement(element);
        for (final Key key : Arrays.asList(keys.getFirst(), keys.getSecond())) {
            if (null != key) {
                final SimpleEntry<Key, Value> expectedElement = new SimpleEntry<>(
                        key,
                        converter.getValueFromElement(element)
                );

                // Check the element exists.
                Scanner scanner = getRow(accStore, expectedElement.getKey().getRow());
                assertEquals(1, Iterables.size(scanner));

                delete(accStore, scanner);

                // Check the element has been deleted.
                assertEquals(0, Iterables.size(scanner));
            }
        }
    }

    private void delete(final AccumuloStore accStore, final Scanner scanner) throws TableNotFoundException, StoreException, MutationsRejectedException {
        Mutation deleter = null;
        for (final Entry<Key, Value> entry : scanner) {
            if (deleter == null) {
                deleter = new Mutation(entry.getKey().getRow());
            }
            deleter.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(), entry.getKey().getTimestamp());
        }
        final BatchWriter batchWriter = accStore.getConnection().createBatchWriter(accStore.getTableName(), new BatchWriterConfig());
        batchWriter.addMutation(deleter);
        batchWriter.flush();
    }

    private Scanner getRow(AccumuloStore accStore, Text row) throws Exception {
        final Scanner scanner = accStore.getConnection().createScanner(accStore.getTableName(), Authorizations.EMPTY);
        scanner.setRange(new Range(row));
        return scanner;
    }
}

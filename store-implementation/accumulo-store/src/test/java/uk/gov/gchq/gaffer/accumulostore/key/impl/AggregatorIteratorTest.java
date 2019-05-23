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

package uk.gov.gchq.gaffer.accumulostore.key.impl;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.MockAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AggregatorIteratorTest {

    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(AggregatorIteratorTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil
            .storeProps(AggregatorIteratorTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties
            .loadStoreProperties(StreamUtil.openStream(AggregatorIteratorTest.class, "/accumuloStoreClassicKeys.properties"));
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;

    @BeforeClass
    public static void setup() throws IOException, StoreException {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
    }

    @Before
    public void reInitialise() throws StoreException {
        byteEntityStore.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        gaffer1KeyStore.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
    }

    @Test
    public void test() throws OperationException {
        test(byteEntityStore);
        test(gaffer1KeyStore);
    }

    private void test(final AccumuloStore store) throws OperationException {
        // Given
        final Edge expectedResult = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("2")
                .directed(true)
                .property(AccumuloPropertyNames.COUNT, 13)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                .property(AccumuloPropertyNames.PROP_1, 0)
                .property(AccumuloPropertyNames.PROP_2, 0)
                .property(AccumuloPropertyNames.PROP_3, 1)
                .property(AccumuloPropertyNames.PROP_4, 1)
                .build();

        final Edge edge1 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("2")
                .directed(true)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                .property(AccumuloPropertyNames.COUNT, 1)
                .property(AccumuloPropertyNames.PROP_1, 0)
                .property(AccumuloPropertyNames.PROP_2, 0)
                .property(AccumuloPropertyNames.PROP_3, 1)
                .property(AccumuloPropertyNames.PROP_4, 0)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("2")
                .directed(true)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                .property(AccumuloPropertyNames.COUNT, 2)
                .property(AccumuloPropertyNames.PROP_1, 0)
                .property(AccumuloPropertyNames.PROP_2, 0)
                .property(AccumuloPropertyNames.PROP_3, 0)
                .property(AccumuloPropertyNames.PROP_4, 1)
                .build();

        final Edge edge3 = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("2")
                .directed(true)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                .property(AccumuloPropertyNames.COUNT, 10)
                .property(AccumuloPropertyNames.PROP_1, 0)
                .property(AccumuloPropertyNames.PROP_2, 0)
                .property(AccumuloPropertyNames.PROP_3, 0)
                .property(AccumuloPropertyNames.PROP_4, 0)
                .build();

        final User user = new User();
        store.execute(new AddElements.Builder()
                .input(edge1, edge2, edge3)
                .build(), new Context(user));

        final GetElements get = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .input(new EntitySeed("1"))
                .build();

        // When
        final List<Element> results = Lists.newArrayList(store.execute(get, new Context(user)));

        // Then
        assertEquals(1, results.size());

        final Edge aggregatedEdge = (Edge) results.get(0);
        assertEquals(expectedResult, aggregatedEdge);
        assertEquals(expectedResult.getProperties(), aggregatedEdge.getProperties());
    }

    @Test
    public void shouldGetGroupFromElementConverter() throws IOException {
        MockAccumuloElementConverter.cleanUp();
        // Given
        MockAccumuloElementConverter.mock = mock(AccumuloElementConverter.class);
        final Key key = mock(Key.class);
        final List<Value> values = Arrays.asList(mock(Value.class), mock(Value.class));
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.ENTITY, new SchemaEdgeDefinition())
                .build();
        final ByteSequence colFamData = mock(ByteSequence.class);
        final byte[] colFam = StringUtil.toBytes(TestGroups.ENTITY);
        final SortedKeyValueIterator sortedKeyValueIterator = mock(SortedKeyValueIterator.class);
        final IteratorEnvironment iteratorEnvironment = mock(IteratorEnvironment.class);
        final Map<String, String> options = new HashMap();

        options.put("columns", "test");
        options.put(AccumuloStoreConstants.SCHEMA, new String(schema.toCompactJson()));
        options.put(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS, MockAccumuloElementConverter.class.getName());

        given(colFamData.getBackingArray()).willReturn(colFam);
        given(key.getColumnFamilyData()).willReturn(colFamData);
        given(MockAccumuloElementConverter.mock.getGroupFromColumnFamily(colFam)).willReturn(TestGroups.ENTITY);

        final AggregatorIterator aggregatorIterator = new AggregatorIterator();

        // When
        aggregatorIterator.init(sortedKeyValueIterator, options, iteratorEnvironment);
        aggregatorIterator.reduce(key, values.iterator());

        // Then
        verify(MockAccumuloElementConverter.mock, times(1)).getGroupFromColumnFamily(colFam);

        MockAccumuloElementConverter.cleanUp();
    }
}

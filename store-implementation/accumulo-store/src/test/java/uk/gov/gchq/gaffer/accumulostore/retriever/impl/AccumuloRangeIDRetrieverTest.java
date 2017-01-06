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

package uk.gov.gchq.gaffer.accumulostore.retriever.impl;

import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccumuloRangeIDRetrieverTest {

    private static final int numEntries = 1000;
    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(AccumuloRangeIDRetrieverTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloRangeIDRetrieverTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloRangeIDRetrieverTest.class, "/accumuloStoreClassicKeys.properties"));

    @BeforeClass
    public static void setup() throws StoreException, IOException {
        byteEntityStore = new MockAccumuloStore();
        gaffer1KeyStore = new MockAccumuloStore();
        byteEntityStore.initialise(schema, PROPERTIES);
        gaffer1KeyStore.initialise(schema, CLASSIC_PROPERTIES);
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
        setupGraph(byteEntityStore, numEntries);
        setupGraph(gaffer1KeyStore, numEntries);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void shouldRetieveElementsInRangeBetweenSeedsByteEntityStore() throws StoreException {
        shouldRetieveElementsInRangeBetweenSeeds(byteEntityStore);
    }

    @Test
    public void shouldRetieveElementsInRangeBetweenSeedsGaffer1Store() throws StoreException {
        shouldRetieveElementsInRangeBetweenSeeds(gaffer1KeyStore);
    }

    private void shouldRetieveElementsInRangeBetweenSeeds(final AccumuloStore store) throws StoreException {
        // Create set to query for
        final Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0000"), new EntitySeed("0999")));

        // Retrieve elements when less simple entities are provided than the max number of entries for the batch scanner
        final GetElementsOperation<Pair<ElementSeed>, CloseableIterable<Element>> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);
        try {
            final AccumuloRangeIDRetriever retriever = new AccumuloRangeIDRetriever(store, operation, new User());
            assertEquals(numEntries, Iterables.size(retriever));
        } catch (IteratorSettingException e) {
            fail("Unable to construct Range Retriever");
        }
    }

    private static void setupGraph(final AccumuloStore store, int numEntries) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            final Edge edge = new Edge(TestGroups.EDGE);
            String s = "" + i;
            while (s.length() < 4) {
                s = "0" + s;
            }
            edge.setSource(s);
            edge.setDestination("B");
            edge.setDirected(false);
            elements.add(edge);
        }
        try {
            final User user = new User();
            store.execute(new AddElements(elements), user);
        } catch (OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }
}
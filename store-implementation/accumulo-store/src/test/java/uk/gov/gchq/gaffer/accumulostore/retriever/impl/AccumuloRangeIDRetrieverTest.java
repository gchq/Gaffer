/*
 * Copyright 2016-2020 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.AccumuloTestClusterManager;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccumuloRangeIDRetrieverTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloRangeIDRetrieverTest.class);
    private static final int NUM_ENTRIES = 1000;
    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(AccumuloRangeIDRetrieverTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloRangeIDRetrieverTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloRangeIDRetrieverTest.class, "/accumuloStoreClassicKeys.properties"));

    private static AccumuloTestClusterManager accumuloTestClusterManagerByteEntity;
    private static AccumuloTestClusterManager accumuloTestClusterManagerGaffer1Key;

    @ClassRule
    public static TemporaryFolder storeBaseFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void setup() throws StoreException {
        File storeFolder1 = null;
        File storeFolder2 = null;
        try {
            storeFolder1 = storeBaseFolder.newFolder();
        } catch (IOException e) {
            LOGGER.error("Failed to create sub folder 1 in : " + storeBaseFolder.getRoot().getAbsolutePath() + ": " + e.getMessage());
        }
        try {
            storeFolder2 = storeBaseFolder.newFolder();
        } catch (IOException e) {
            LOGGER.error("Failed to create sub folder 2 in : " + storeBaseFolder.getRoot().getAbsolutePath() + ": " + e.getMessage());
        }
        accumuloTestClusterManagerByteEntity = new AccumuloTestClusterManager(PROPERTIES, storeFolder1.getAbsolutePath());
        accumuloTestClusterManagerGaffer1Key = new AccumuloTestClusterManager(CLASSIC_PROPERTIES, storeFolder2.getAbsolutePath());

        byteEntityStore = new SingleUseAccumuloStore();
        gaffer1KeyStore = new SingleUseAccumuloStore();
        byteEntityStore.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        gaffer1KeyStore.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
        setupGraph(byteEntityStore, NUM_ENTRIES);
        setupGraph(gaffer1KeyStore, NUM_ENTRIES);
    }

    @AfterClass
    public static void tearDown() {
        accumuloTestClusterManagerByteEntity.close();
        accumuloTestClusterManagerGaffer1Key.close();
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void shouldRetrieveElementsInRangeBetweenSeedsByteEntityStore() throws Exception {
        shouldRetrieveElementsInRangeBetweenSeeds(byteEntityStore);
    }

    @Test
    public void shouldRetrieveElementsInRangeBetweenSeedsGaffer1Store() throws Exception {
        shouldRetrieveElementsInRangeBetweenSeeds(gaffer1KeyStore);
    }

    private void shouldRetrieveElementsInRangeBetweenSeeds(final AccumuloStore store) throws Exception {
        // Create set to query for
        final Set<Pair<ElementId, ElementId>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0000"), new EntitySeed("0999")));

        // Retrieve elements when less simple entities are provided than the max number of entries for the batch scanner
        final GetElementsInRanges operation = new GetElementsInRanges.Builder()
                .view(defaultView)
                .input(simpleEntityRanges)
                .build();
        final AccumuloRangeIDRetriever<?> retriever = new AccumuloRangeIDRetriever<>(store, operation, new User());
        final List<Element> elements = Lists.newArrayList(retriever);
        for (final Element element : elements) {
            if (element instanceof Edge) {
                assertEquals(EdgeId.MatchedVertex.SOURCE, ((Edge) element).getMatchedVertex());
            }
        }

        assertEquals(NUM_ENTRIES, elements.size());
    }

    private static void setupGraph(final AccumuloStore store, final int numEntries) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            String s = "" + i;
            while (s.length() < 4) {
                s = "0" + s;
            }

            elements.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(s)
                    .dest("B")
                    .directed(false)
                    .build());
        }
        try {
            final User user = new User();
            store.execute(new AddElements.Builder()
                    .input(elements)
                    .build(), new Context(user));
        } catch (final OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }
}

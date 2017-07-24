/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.user.User;
import java.util.HashSet;
import java.util.Set;

public class FederatedStoreITsWithoutAutomation {
    private static StoreProperties STORE_PROPERTIES;
    private FederatedStore federatedStore;

    @Before
    public void setUp() throws Exception {
        STORE_PROPERTIES = StoreProperties.loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, "/integration-test-federated.properties"));
        federatedStore = new FederatedStore();
    }

    @Test
    public void shouldAddTwoGraphs() throws Exception {
        // When
        int sizeBefore = federatedStore.getGraphs().size();
        federatedStore.initialise("testFederatedStore", null, STORE_PROPERTIES);
        int sizeAfter = federatedStore.getGraphs().size();

        //Then
        assertEquals(0, sizeBefore);
        assertEquals(2, sizeAfter);
    }

    @Test
    public void shouldCombineTraits() throws Exception {
        //Given
        HashSet<StoreTrait> traits = new HashSet<>();
        traits.addAll(SingleUseAccumuloStore.TRAITS);
        traits.addAll(MapStore.TRAITS);
        int expectedSize = traits.size();

        //When
        int sizeBefore = federatedStore.getTraits().size();
        federatedStore.initialise("testFederatedStore", null, STORE_PROPERTIES);
        int sizeAfter = federatedStore.getTraits().size();

        //Then
        assertEquals(0, sizeBefore);
        assertNotEquals(SingleUseAccumuloStore.TRAITS, MapStore.TRAITS);
        assertEquals(expectedSize, sizeAfter);
    }

    @Test
    public void shouldContainNoElements() throws Exception {
        Set<Element> elementsBefore = getElements();
        federatedStore.initialise("testFederatedStore", null, STORE_PROPERTIES);
        Set<Element> elementsAfter = getElements();
        assertEquals(0, elementsBefore.size());
        assertEquals(0, elementsAfter.size());
    }

    @Test
    public void shouldAddEdgesToOneGraph() throws Exception {
        federatedStore.initialise("testFederatedStore", null, STORE_PROPERTIES);

        AddElements op = new AddElements.Builder()
                .input(new Edge.Builder()
                               .group("BasicEdge")
                               .source("testSource")
                               .dest("testDest")
                               .property("property1", "testProp")
                               .build())
                .build();

        federatedStore.execute(op, new User("testUser"));

        assertEquals(1, getElements().size());

    }

    private Set<Element> getElements() throws uk.gov.gchq.gaffer.operation.OperationException {
        Set<Element> elementSet = Sets.newHashSet();
        for (Graph graph : federatedStore.getGraphs()) {
            CloseableIterable<? extends Element> elements = graph.execute(new GetAllElements(), new User("testUser"));
            for (Element element : elements) {
                elementSet.add(element);
            }
        }
        return elementSet;
    }
}

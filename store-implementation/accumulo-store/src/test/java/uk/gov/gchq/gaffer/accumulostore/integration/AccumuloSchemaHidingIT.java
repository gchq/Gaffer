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
package uk.gov.gchq.gaffer.accumulostore.integration;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.graph.SchemaHidingIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

import java.util.List;

public class AccumuloSchemaHidingIT extends SchemaHidingIT {
    public AccumuloSchemaHidingIT() {
        super("mockAccumuloStore.properties");
    }

    @Override
    protected void cleanUp() {
        final AccumuloProperties storeProps = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(getClass(), storePropertiesPath));

        final AccumuloStore store;
        try {
            store = Class.forName(storeProps.getStoreClass()).asSubclass(AccumuloStore.class).newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not create store of type: " + storeProps.getStoreClass(), e);
        }

        try {
            store.preInitialise(
                    "graphId",
                    createFullSchema(),
                    storeProps
            );
            store.getConnection().tableOperations().delete(store.getTableName());
        } catch (final Exception e) {
            // ignore exceptions
        }
    }

    @Override
    protected void testOperations(final Graph fullGraph, final Graph filteredGraph, final List<Edge> fullExpectedResults, final List<Edge> filteredExpectedResults) throws OperationException {
        super.testOperations(fullGraph, filteredGraph, fullExpectedResults, filteredExpectedResults);

        final GetElementsInRanges getElementsInRange = new GetElementsInRanges.Builder()
                .input(new Pair<>(new EntitySeed("a"), new EntitySeed("z")))
                .build();
        testOperation(fullGraph, filteredGraph, getElementsInRange, fullExpectedResults, filteredExpectedResults);

        final GetElementsBetweenSets getElementsBetweenSets = new GetElementsBetweenSets.Builder()
                .input(new EntitySeed("source1a"),
                        new EntitySeed("source1b"),
                        new EntitySeed("source2"))
                .inputB(new EntitySeed("dest1a"),
                        new EntitySeed("dest1b"),
                        new EntitySeed("dest2"))
                .build();
        testOperation(fullGraph, filteredGraph, getElementsBetweenSets, fullExpectedResults, filteredExpectedResults);

        final GetElementsWithinSet getElementsWithinSet = new GetElementsWithinSet.Builder()
                .input(new EntitySeed("source1a"),
                        new EntitySeed("source1b"),
                        new EntitySeed("source2"),
                        new EntitySeed("dest1a"),
                        new EntitySeed("dest1b"),
                        new EntitySeed("dest2"))
                .build();
        testOperation(fullGraph, filteredGraph, getElementsWithinSet, fullExpectedResults, filteredExpectedResults);
    }
}

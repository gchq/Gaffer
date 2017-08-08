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
package uk.gov.gchq.gaffer.doc.dev.walkthrough;

import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.doc.user.generator.RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphID;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

public class FederatedStoreWalkThrough extends DevWalkthrough {
    public FederatedStoreWalkThrough() {
        super("FederatedStore", "RoadAndRoadUseWithTimesAndCardinalities");
    }

    public CloseableIterable<? extends Element> run() throws Exception {
        // [federated store] create a store that federates to a MapStore and AccumuloStore
        // ---------------------------------------------------------
        final FederatedStore federatedStore = new FederatedStore();
        StoreProperties properties = StoreProperties.loadStoreProperties("federatedStore.properties");
        federatedStore.initialise("federatedRoadUse", properties);
        // ---------------------------------------------------------

        final User user = new User("user01");

        // [add graph] add a graph to the federated store.
        // ---------------------------------------------------------
        AddGraph addAnotherGraph = new AddGraph.Builder()
                .setGraphId("AnotherGraph")
                .setSchema(new Schema.Builder()
                                   .json(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/schema"))
                                   .build())
                .setStoreProperties(StoreProperties.loadStoreProperties("mockmapstore.properties"))
                .build();
        federatedStore.execute(addAnotherGraph, user);
        // ---------------------------------------------------------

        // [remove graph] remove a graph from the federated store.
        // ---------------------------------------------------------
        RemoveGraph removeGraph = new RemoveGraph.Builder()
                .setGraphId("AnotherGraph")
                .build();
        federatedStore.execute(removeGraph, user);
        // ---------------------------------------------------------

        // [getallgraphid] Get a list of all the graphID within the FederatedStore.
        // ---------------------------------------------------------
        Iterable<? extends String> graphIds = federatedStore.execute(new GetAllGraphID(), user);
        // ---------------------------------------------------------

        // [add elements] Create a data generator and add the edges to the federated graphs using an operation chain consisting of:
        // generateElements - generating edges from the data (note these are directed edges)
        // addElements - add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                               .generator(new RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator())
                               .input(IOUtils.readLines(StreamUtil.openStream(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/data.txt")))
                               .build())
                .then(new AddElements())
                .build();

        federatedStore.execute(addOpChain, user);
        // ---------------------------------------------------------

        // [get elements]
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<? extends Element>> getOpChain = new OperationChain.Builder()
                .first(new GetAllElements.Builder()
                               .build())
                .build();

        CloseableIterable<? extends Element> allElements = federatedStore.execute(getOpChain, user);
        // ---------------------------------------------------------


        return allElements;
    }

    public static void main(final String[] args) throws Exception {
        final FederatedStoreWalkThrough walkthrough = new FederatedStoreWalkThrough();
        walkthrough.run();
    }
}

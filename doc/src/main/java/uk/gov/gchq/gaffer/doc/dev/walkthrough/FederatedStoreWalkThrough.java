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
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

public class FederatedStoreWalkThrough extends DevWalkthrough {
    public FederatedStoreWalkThrough() {
        super("FederatedStore", "RoadAndRoadUseWithTimesAndCardinalitiesForFederatedStore");
    }

    public static void main(final String[] args) throws Exception {
        final FederatedStoreWalkThrough walkthrough = new FederatedStoreWalkThrough();
        walkthrough.run();
    }

    public CloseableIterable<? extends Element> run() throws Exception {
        // [federated store] create a store that federates to a MapStore and AccumuloStore
        // ---------------------------------------------------------
        final Graph federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("federatedRoadUse")
                        .library(new HashMapGraphLibrary())
                        .build())
                .storeProperties(StreamUtil.openStream(getClass(), "federatedStore.properties"))
                .build();

        // ---------------------------------------------------------

        final User user = new User("user01");

        // [add graph] add a graph to the federated store.
        // ---------------------------------------------------------
        AddGraph addAnotherGraph = new AddGraph.Builder()
                .setGraphId("AnotherGraph")
                .schema(new Schema.Builder()
                        .json(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithTimesAndCardinalitiesForFederatedStore/schema"))
                        .build())
                .storeProperties(StoreProperties.loadStoreProperties("mockmapstore.properties"))
                .build();
        federatedGraph.execute(addAnotherGraph, user);
        // ---------------------------------------------------------

        log("addGraphJson", new String(JSONSerialiser.serialise(addAnotherGraph, true)));

        // [remove graph] remove a graph from the federated store.
        // ---------------------------------------------------------
        RemoveGraph removeGraph = new RemoveGraph.Builder()
                .setGraphId("AnotherGraph")
                .build();
        federatedGraph.execute(removeGraph, user);
        // ---------------------------------------------------------

        log("removeGraphJson", new String(JSONSerialiser.serialise(removeGraph, true)));

        // [get all graph ids] Get a list of all the graphId within the FederatedStore.
        // ---------------------------------------------------------
        final GetAllGraphIds getAllGraphIDs = new GetAllGraphIds();
        Iterable<? extends String> graphIds = federatedGraph.execute(getAllGraphIDs, user);
        // ---------------------------------------------------------

        log("getAllGraphIdsJson", new String(JSONSerialiser.serialise(getAllGraphIDs, true)));

        log("graphIds", graphIds.toString());


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

        federatedGraph.execute(addOpChain, user);
        // ---------------------------------------------------------

        // [get elements]
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<? extends Element>> getOpChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("10"))
                        .build())
                .build();

        CloseableIterable<? extends Element> elements = federatedGraph.execute(getOpChain, user);
        // ---------------------------------------------------------

        for (final Element element : elements) {
            log("elements", element.toString());
        }

        // [get elements from accumulo graph]
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<? extends Element>> getOpChainOnAccumuloGraph = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("10"))
                        .option(FederatedStoreConstants.GRAPH_IDS, "accumuloGraph")
                        .build())
                .build();

        CloseableIterable<? extends Element> elementsFromAccumuloGraph = federatedGraph.execute(getOpChainOnAccumuloGraph, user);
        // ---------------------------------------------------------

        for (final Element element : elementsFromAccumuloGraph) {
            log("elements from accumuloGraph", element.toString());
        }

        // [add secure graph] add a graph to the federated store.
        // ---------------------------------------------------------
        AddGraph addSecureGraph = new AddGraph.Builder()
                .setGraphId("SecureGraph")
                .schema(new Schema.Builder()
                        .json(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/schema"))
                        .build())
                .storeProperties(StoreProperties.loadStoreProperties("mockmapstore.properties"))
                .graphAuths("public", "private")
                .build();
        federatedGraph.execute(addSecureGraph, user);
        // ---------------------------------------------------------

        log("addSecureGraphJson", new String(JSONSerialiser.serialise(addSecureGraph, true)));


        return elements;
    }
}

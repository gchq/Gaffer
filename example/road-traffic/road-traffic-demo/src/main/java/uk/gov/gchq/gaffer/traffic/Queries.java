/*
 * Copyright 2018 Crown Copyright
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
package uk.gov.gchq.gaffer.traffic;

import org.apache.commons.io.IOUtils;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.traffic.generator.RoadTrafficStringElementGenerator;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;

import static uk.gov.gchq.gaffer.data.elementdefinition.view.View.createView;
import static uk.gov.gchq.gaffer.operation.impl.get.OperationLibrary.deduplicate;
import static uk.gov.gchq.gaffer.operation.impl.get.OperationLibrary.getEntities;
import static uk.gov.gchq.gaffer.operation.impl.get.OperationLibrary.hop;
import static uk.gov.gchq.gaffer.operation.impl.get.OperationLibrary.input;
import static uk.gov.gchq.gaffer.operation.impl.get.OperationLibrary.sort;
import static uk.gov.gchq.gaffer.operation.impl.get.OperationLibrary.toCsv;
import static uk.gov.gchq.gaffer.types.function.TypeFunctionLibrary.freqMapExtractor;
import static uk.gov.gchq.koryphe.impl.predicate.PredicateLibrary.inDateRange;
import static uk.gov.gchq.koryphe.impl.predicate.PredicateLibrary.isMoreThan;
import static uk.gov.gchq.koryphe.impl.predicate.PredicateLibrary.predicateMap;

/**
 * This class runs simple java queries against the road traffic graph.
 */
public class Queries {
    public static void main(final String[] args) throws OperationException, IOException {
        new Queries().run();
    }

    private void run() throws OperationException, IOException {
        final User user = new User("user01");
        final Graph graph = createGraph(user);

        // Get the schema
        System.out.println(graph.getSchema().toString());

        // Full example
        runFullExample(graph, user);
    }

    private void runFullExample(final Graph graph, final User user) throws OperationException {
        final OperationChain<Iterable<String>> opChain = input("South West")
                .then(hop("RegionContainsLocation"))
                .then(hop("LocationContainsRoad"))
                .then(deduplicate())
                .then(hop("RoadHasJunction"))
                .then(getEntities("JunctionUse")
                        .view(createView()
                                .filter("startDate", inDateRange().start("2000/01/01").end("2001/01/01"))
                                .summarise()
                                .filter("countByVehicleType", predicateMap().key("BUS").predicate(isMoreThan().value(1000L)))
                                .transform("countByVehicleType", freqMapExtractor().key("BUS"), "busCount")))
                .then(sort()
                        .groups("JunctionUse")
                        .property("busCount")
                        .reverse()
                        .limit(2)
                        .deduplicate())
                .then(toCsv()
                        .vertex("Junction")
                        .property("busCount", "Bus Count"));


        final Iterable<? extends String> results = graph.execute(opChain, user);

        System.out.println("Full example results:");
        for (final String result : results) {
            System.out.println(result);
        }
    }

    private Graph createGraph(final User user) throws IOException, OperationException {
        final Graph graph = new Builder()
                .config(new GraphConfig.Builder()
                        .graphId("roadTraffic")
                        .build())
                .addSchemas(StreamUtil.openStreams(ElementGroup.class, "schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "accumulo/store.properties"))
                .build();

        final OperationChain<Void> populateChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .input(IOUtils.readLines(StreamUtil.openStream(getClass(), "roadTrafficSampleData.csv")))
                        .generator(new RoadTrafficStringElementGenerator())
                        .build())
                .then(new AddElements.Builder()
                        .skipInvalidElements(false)
                        .build())
                .build();
        graph.execute(populateChain, user);

        return graph;
    }
}

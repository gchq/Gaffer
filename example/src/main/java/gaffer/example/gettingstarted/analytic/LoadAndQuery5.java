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
package gaffer.example.gettingstarted.analytic;

import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.example.gettingstarted.generator.DataGenerator5;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery5 extends LoadAndQuery {
    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery5().run();
    }

    public Iterable<Edge> run() throws OperationException {

        setDataFileLocation("/example/gettingstarted/5/data.txt");
        setSchemaFolderLocation("/example/gettingstarted/5/schema");
        setStorePropertiesLocation("/example/gettingstarted/mockaccumulostore.properties");

        final Graph graph5 = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();

        final List<Element> elements = new ArrayList<>();
        final DataGenerator5 dataGenerator5 = new DataGenerator5();
        log("\nTurn the data into Graph Edges\n");
        for (String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator5.getElement(s));
            log(dataGenerator5.getElement(s).toString());
        }
        log("");

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();

        final User basicUser = new User("basicUser");
        graph5.execute(addElements, basicUser);

        final GetRelatedEdges<EntitySeed> getRelatedEdges = new GetRelatedEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();

        log("\nNow run a simple query to get edges\n");
        final Iterable<Edge> results = graph5.execute(getRelatedEdges, basicUser);
        for (Element e : results) {
            log(e.toString());
        }
        log("We get nothing back");

        final User privateUser = new User.Builder()
                .userId("privateUser")
                .dataAuth("private")
                .build();
        log("\nGet edges with the private visibility. We should get the public edges too\n");
        final Iterable<Edge> privatePublicResults = graph5.execute(getRelatedEdges, privateUser);
        for (Element e : privatePublicResults) {
            log(e.toString());
        }

        final User publicUser = new User.Builder()
                .userId("publicUser")
                .dataAuth("public")
                .build();
        log("\nGet edges with the public visibility. We shouldn't see any of the private ones. Notice that the Edges are aggregated within visibilities\n");
        final Iterable<Edge> publicResults = graph5.execute(getRelatedEdges, publicUser);
        for (Element e : publicResults) {
            log(e.toString());
        }

        getRelatedEdges.setSummarise(true);
        log("\nGet edges with the private visibility again but this time, aggregate the visibilities based on the rules in gaffer.example.gettingstarted.function.VisibilityAggregator.\n");
        final Iterable<Edge> privatePublicAggregatedResults = graph5.execute(getRelatedEdges, privateUser);
        for (Element e : privatePublicAggregatedResults) {
            log(e.toString());
        }

        return privatePublicAggregatedResults;
    }
}

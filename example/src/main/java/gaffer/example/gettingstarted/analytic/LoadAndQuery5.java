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

import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.data.element.Element;
import gaffer.example.gettingstarted.generator.DataGenerator5;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery5 extends LoadAndQuery {
    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery5().run();
    }

    public void run() throws OperationException {

        setDataFileLocation("/example/gettingstarted/data/data5.txt");
        setDataSchemaLocation("/example/gettingstarted/schema5/dataSchema.json");
        setDataTypesLocation("/example/gettingstarted/schema5/dataTypes.json");
        setStoreTypesLocation("/example/gettingstarted/schema5/storeTypes.json");
        setStorePropertiesLocation("/example/gettingstarted/properties/mockaccumulostore.properties");

        Graph graph3 = new Graph.Builder()
                .addSchema(getDataSchema())
                .addSchema(getDataTypes())
                .addSchema(getStoreTypes())
                .storeProperties(getStoreProperties())
                .build();

        List<Element> elements = new ArrayList<>();
        DataGenerator5 dataGenerator5 = new DataGenerator5();
        System.out.println("\nTurn the data into Graph Edges\n");
        for (String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator5.getElement(s));
            System.out.println(dataGenerator5.getElement(s).toString());
        }
        System.out.println("");

        AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();

        graph3.execute(addElements);

        GetRelatedEdges getRelatedEdges = new GetRelatedEdges.Builder()
                .addSeed(new EntitySeed("1"))
                .build();

        System.out.println("\nNow run a simple query to get edges\n");
        for (Element e : graph3.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }
        System.out.println("We get nothing back");

        getRelatedEdges.addOption(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, "private");

        System.out.println("\nGet edges with the private visibility. We should get the public edges too\n");
        for (Element e : graph3.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }

        getRelatedEdges.addOption(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, "public");

        System.out.println("\nGet edges with the public visibility. We shouldn't see any of the private ones. Notice that the Edges are aggregated within visibilities\n");
        for (Element e : graph3.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }

        getRelatedEdges.addOption(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, "private");
        getRelatedEdges.setSummarise(true);

        System.out.println("\nGet edges with the private visibility again but this time, aggregate the visibilities based on the rules in gaffer.example.gettingstarted.function.VisibilityAggregator.\n");
        for (Element e : graph3.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }


    }

}

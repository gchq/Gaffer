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

import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.example.gettingstarted.generator.DataGenerator3;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery3 extends LoadAndQuery {
    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery3().run();
    }

    public void run() throws OperationException {

        setDataFileLocation("/example/gettingstarted/data/data3.txt");
        setDataSchemaLocation("/example/gettingstarted/schema3/dataSchema.json");
        setDataTypesLocation("/example/gettingstarted/schema3/dataTypes.json");
        setStoreTypesLocation("/example/gettingstarted/schema3/storeTypes.json");
        setStorePropertiesLocation("/example/gettingstarted/properties/mockaccumulostore.properties");

        List<Element> elements = new ArrayList<>();
        DataGenerator3 dataGenerator3 = new DataGenerator3();
        for (String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator3.getElement(s));
            System.out.println(dataGenerator3.getElement(s).toString());
        }
        System.out.println("");

        Graph graph3 = new Graph.Builder()
                .addSchema(getDataSchema())
                .addSchema(getDataTypes())
                .addSchema(getStoreTypes())
                .storeProperties(getStoreProperties())
                .build();

        AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();

        graph3.execute(addElements);

        GetRelatedEdges getRelatedEdges = new GetRelatedEdges.Builder()
                .addSeed(new EntitySeed("1"))
                .build();

        System.out.println("\nAll edges containing the vertex 1. The counts have been aggregated\n");
        for (Element e : graph3.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }

        ViewElementDefinition viewElementDefinition = new ViewElementDefinition.Builder()
                .filter(new ElementFilter.Builder()
                        .select("count")
                        .execute(new IsMoreThan(3))
                        .build())
                .build();


        View view = new View.Builder()
                .edge("data1", viewElementDefinition)
                .build();

        getRelatedEdges.setView(view);

        System.out.println("\nAll edges containing the vertex 1. "
                + "\nThe counts have been aggregated and we have filtered out edges where the count is less than or equal to 3\n");
        for (Element e : graph3.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }
    }
}

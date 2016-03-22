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
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.example.gettingstarted.function.MeanTransform;
import gaffer.example.gettingstarted.generator.DataGenerator5;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.function.simple.filter.IsMoreThan;
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
        Graph graph5 = new Graph.Builder()
                .addSchema(getDataSchema(5))
                .addSchema(getDataTypes(5))
                .addSchema(getStoreTypes(5))
                .storeProperties(getStoreProperties())
                .build();

        List<Element> elements = new ArrayList<>();
        DataGenerator5 dataGenerator5 = new DataGenerator5();
        for (String s : DataUtils.loadData(getData(5))) {
            elements.add(dataGenerator5.getElement(s));
        }

        AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();

        graph5.execute(addElements);

        GetRelatedEdges getRelatedEdges = new GetRelatedEdges.Builder()
                .addSeed(new EntitySeed("1"))
                .build();

        System.out.println("\nAll edges containing the vertex 1. The counts and 'things' have been aggregated\n");
        for (Element e : graph5.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }

        ElementTransformer mean = new ElementTransformer.Builder()
                .select("thing", "count")
                .project("mean")
                .execute(new MeanTransform())
                .build();

        ElementFilter filter = new ElementFilter.Builder()
                .select("mean")
                .execute(new IsMoreThan(30))
                .build();

        ViewElementDefinition viewElementDefinition = new ViewElementDefinition.Builder()
                .transientProperty("mean", Float.class)
                .transformer(mean)
                .filter(filter)
                .build();

        View view = new View.Builder()
                .edge("data", viewElementDefinition)
                .build();

        getRelatedEdges.setView(view);

        System.out.println("\nAll edges containing the vertex 1. We can add a new property to the edges that is calculated from the aggregated values of other properties\n");
        for (Element e : graph5.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }

        System.out.println("\nAll edges containing the vertex 1. We can add a new property to the edges that is calculated from the aggregated values of other properties\n");
        for (Element e : graph5.execute(getRelatedEdges)) {
            System.out.println(e.toString());
        }
    }
}

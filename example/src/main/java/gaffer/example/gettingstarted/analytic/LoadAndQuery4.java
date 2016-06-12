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
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.example.gettingstarted.function.transform.MeanTransform;
import gaffer.example.gettingstarted.generator.DataGenerator4;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery4 extends LoadAndQuery {
    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery4().run();
    }

    public Iterable<Edge> run() throws OperationException {
        final User user = new User("user01");

        setDataFileLocation("/example/gettingstarted/4/data.txt");
        setSchemaFolderLocation("/example/gettingstarted/4/schema");
        setStorePropertiesLocation("/example/gettingstarted/mockaccumulostore.properties");

        final List<Element> elements = new ArrayList<>();
        final DataGenerator4 dataGenerator4 = new DataGenerator4();
        for (String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator4.getElement(s));
            log(dataGenerator4.getElement(s).toString());
        }
        log("");

        final Graph graph4 = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();

        graph4.execute(addElements, user);

        final GetRelatedEdges<EntitySeed> getRelatedEdges = new GetRelatedEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();

        log("\nAll edges containing the vertex 1. The counts and 'things' have been aggregated\n");
        final Iterable<Edge> results = graph4.execute(getRelatedEdges, user);
        for (Element e : results) {
            log(e.toString());
        }

        final ElementTransformer mean = new ElementTransformer.Builder()
                .select("thing", "count")
                .project("mean")
                .execute(new MeanTransform())
                .build();

        final ViewElementDefinition viewElementDefinition = new ViewElementDefinition.Builder()
                .transientProperty("mean", Float.class)
                .transformer(mean)
                .build();

        final View view = new View.Builder()
                .edge("data", viewElementDefinition)
                .build();

        getRelatedEdges.setView(view);

        log("\nWe can add a new property to the edges that is calculated from the aggregated values of other properties\n");
        final Iterable<Edge> transientResults = graph4.execute(getRelatedEdges, user);
        for (Element e : transientResults) {
            log(e.toString());
        }

        return transientResults;
    }
}

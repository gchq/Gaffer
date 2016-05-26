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
import gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery3 extends LoadAndQuery {
    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery3().run();
    }

    public Iterable<Edge> run() throws OperationException {
        final User user = new User("user01");

        setDataFileLocation("/example/gettingstarted/3/data.txt");
        setSchemaFolderLocation("/example/gettingstarted/3/schema");
        setStorePropertiesLocation("/example/gettingstarted/mockaccumulostore.properties");

        final List<Element> elements = new ArrayList<>();
        final DataGenerator3 dataGenerator3 = new DataGenerator3();
        for (String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator3.getElement(s));
            log(dataGenerator3.getElement(s).toString());
        }
        log("");

        final Graph graph3 = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();

        graph3.execute(addElements, user);

        final GetRelatedEdges<EntitySeed> getRelatedEdges = new GetRelatedEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();

        log("\nAll edges containing the vertex 1. The counts have been aggregated\n");
        final Iterable<Edge> results = graph3.execute(getRelatedEdges, user);
        for (Element e : results) {
            log(e.toString());
        }

        final ViewElementDefinition viewElementDefinition = new ViewElementDefinition.Builder()
                .filter(new ElementFilter.Builder()
                        .select("count")
                        .execute(new IsMoreThan(3))
                        .build())
                .build();

        final View view = new View.Builder()
                .edge("data", viewElementDefinition)
                .build();

        getRelatedEdges.setView(view);

        log("\nAll edges containing the vertex 1. "
                + "\nThe counts have been aggregated and we have filtered out edges where the count is less than or equal to 3\n");
        final Iterable<Edge> filteredResults = graph3.execute(getRelatedEdges, user);
        for (Element e : filteredResults) {
            log(e.toString());
        }

        return filteredResults;

    }
}

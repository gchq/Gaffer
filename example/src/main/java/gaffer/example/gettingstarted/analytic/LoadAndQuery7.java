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

import com.google.common.collect.Lists;
import gaffer.data.AlwaysValid;
import gaffer.data.IsEdgeValidator;
import gaffer.data.element.Element;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.example.gettingstarted.generator.DataGenerator7;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.graph.Graph;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.data.generator.EntitySeedExtractor;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.export.FetchExport;
import gaffer.operation.impl.export.UpdateExport;
import gaffer.operation.impl.export.initialise.InitialiseSetExport;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.user.User;
import java.util.List;

public class LoadAndQuery7 extends LoadAndQuery {

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery7().run();
    }

    public Iterable<Element> run() throws OperationException {
        final User user = new User("user01");

        setDataFileLocation("/example/gettingstarted/7/data.txt");
        setSchemaFolderLocation("/example/gettingstarted/7/schema");
        setStorePropertiesLocation("/example/gettingstarted/mockaccumulostore.properties");

        //create a graph using our schema and store properties
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();

        // Create data generator
        final DataGenerator7 dataGenerator = new DataGenerator7();

        // Load data into memory
        final List<String> data = DataUtils.loadData(getData());

        //add the edges to the graph using an operation chain consisting of:
        //generateElements - generating edges from the data (note these are directed edges)
        //addElements - add the edges to the graph
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(dataGenerator)
                        .objects(data)
                        .build())
                .then(new AddElements())
                .build();

        // Execute the add operation chain
        graph.execute(addOpChain, user);

        // Create some starting seeds for the sub graph.
        final Iterable<EntitySeed> seeds = Lists.newArrayList(new EntitySeed("1"));

        // The number of hops around the graph, this is the number of edges to traverse along.
        final int hops = 2;

        // Create a view to filter out elements.
        // This view will return all entities with group 'entity' and all edges
        // with group 'edge' that have a count <= 1
        // Note we could have used a different view for each hop in order to
        // specify the edges we wish to hop down or to attempt to prevent caching
        // duplicate edges and entities.
        final View view = new View.Builder()
                .entity("entity")
                .edge("edge", new ViewElementDefinition.Builder()
                        .filter(new ElementFilter.Builder()
                                .select("count")
                                .execute(new IsMoreThan(1))
                                .build())
                        .build())
                .build();

        // Create a sub graph using an operation chain
        final OperationChain<Iterable<Element>> subGraphOpChain = createSubGraphOpChain(seeds, hops, view);

        // Execute the sub graph operation chain
        final Iterable<Element> subGraph = graph.execute(subGraphOpChain, user);
        log("\nSub graph:");
        for (final Element result : subGraph) {
            log(result.toString());
        }

        return subGraph;
    }

    private OperationChain<Iterable<Element>> createSubGraphOpChain(
            final Iterable<EntitySeed> seeds, final int hops, final View view)
            throws OperationException {

        // This generator will extract just the destination vertices from edges
        // and skip any entities.
        final EntitySeedExtractor destVerticesExtractor = new EntitySeedExtractor(
                new IsEdgeValidator(),
                new AlwaysValid<EntitySeed>(),
                true,
                IdentifierType.DESTINATION);

        // Start the operation chain by initialising the export to use a set.
        // Then do a get related elements with the given seeds.
        // Then update the export with the results
        OperationChain.TypelessBuilder builder = new OperationChain.Builder()
                .first(new InitialiseSetExport())
                .then(new GetRelatedElements.Builder<EntitySeed, Element>()
                        .seeds(seeds)
                        .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                        .view(view)
                        .build())
                .then(new UpdateExport());

        // For each additional hop, extract the destination vertices of the
        // previous edges. Then again get the related elements to these vertices.
        // Then update the export with the results.
        for (int i = 1; i < hops; i++) {
            builder = builder
                    .then(new GenerateObjects<>(destVerticesExtractor))
                    .then(new GetRelatedElements.Builder<EntitySeed, Element>()
                            .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                            .view(view)
                            .build())
                    .then(new UpdateExport());
        }

        // Finally finish off by getting the entities at the destination of the
        // previous edges.
        // Update the export.
        // Then return all the elements in the export.
        final OperationChain opChain = builder
                .then(new GenerateObjects<>(destVerticesExtractor))
                .then(new GetEntitiesBySeed())
                .then(new UpdateExport())
                .then(new FetchExport())
                .build();

        return (OperationChain<Iterable<Element>>) opChain;
    }
}

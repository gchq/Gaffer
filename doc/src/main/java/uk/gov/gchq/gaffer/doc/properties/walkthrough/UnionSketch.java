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
package uk.gov.gchq.gaffer.doc.properties.walkthrough;

import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.doc.properties.generator.UnionElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.Collections;
import java.util.Set;

public class UnionSketch extends PropertiesWalkthrough {
    public UnionSketch() {
        super(Union.class, "properties/unionSketch", UnionElementGenerator.class);
    }

    public static void main(final String[] args) throws OperationException {
        new UnionSketch().run();
    }

    @Override
    public CloseableIterable<Element> run() throws OperationException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "properties/unionSketch/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();
        // ---------------------------------------------------------


        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [add] addElements - add the edges to the graph
        // ---------------------------------------------------------
        final Set<String> dummyData = Collections.singleton("");
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new UnionElementGenerator())
                        .input(dummyData)
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------
        log("Added 1000 edges A-B0, A-B1, ..., A-B999 on 10/1/17. For each edge we create an Entity with a union sketch"
                + " containing a string of the source and destination from the edge. Added 500 edges A-B750, A-B751, "
                + "..., A-B1249 for day 11/1/17. Again for each edge we create an Entity with a union sketch.");


        // [get entities] Get the entities for separate days
        // ---------------------------------------------------------
        final GetAllElements get = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity("size")
                        .build())
                .build();
        final CloseableIterable<? extends Element> entities = graph.execute(get, user);
        for (final Element entity : entities) {
            log("GET_ENTITIES", entity.toString());
        }
        // ---------------------------------------------------------


        // [get estimate separate days] Get the estimates out of the sketches for the separate days
        // ---------------------------------------------------------
        final GetAllElements getAllEntities2 = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity("size")
                        .build())
                .build();
        final CloseableIterable<? extends Element> allEntities2 = graph.execute(getAllEntities2, user);
        final CloseableIterator<? extends Element> it = allEntities2.iterator();
        final Element entityDay1 = it.next();
        final CompactSketch sketchDay1 = ((Union) entityDay1.getProperty("size")).getResult();
        final Element entityDay2 = it.next();
        final CompactSketch sketchDay2 = ((Union) entityDay2.getProperty("size")).getResult();
        final double estimateDay1 = sketchDay1.getEstimate();
        final double estimateDay2 = sketchDay2.getEstimate();
        // ---------------------------------------------------------
        log("\nThe estimates for the different days");
        log("GET_ESTIMATE_OVER_SEPARATE_DAYS", "" + estimateDay1);
        log("GET_ESTIMATE_OVER_SEPARATE_DAYS", "" + estimateDay2);


        // [get intersection across days] Get the number of edges in common across the two days
        // ---------------------------------------------------------
        final Intersection intersection = Sketches.setOperationBuilder().buildIntersection();
        intersection.update(sketchDay1);
        intersection.update(sketchDay2);
        final double intersectionSizeEstimate = intersection.getResult().getEstimate();
        // ---------------------------------------------------------
        log("\nThe estimate of the number of edges in common across the different days");
        log("PRINT_ESTIMATE", "" + intersectionSizeEstimate);


        // [get union across all days] Get the total number edges across the two days
        // ---------------------------------------------------------
        final GetAllElements getAllEntities = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity("size", new ViewElementDefinition.Builder()
                                .groupBy() // set the group by properties to 'none'
                                .build())
                        .build())
                .build();
        final CloseableIterable<? extends Element> allEntities = graph.execute(getAllEntities, user);
        final Element entity = allEntities.iterator().next();
        final double unionSizeEstimate = ((Union) entity.getProperty("size")).getResult().getEstimate();
        // ---------------------------------------------------------
        log("\nThe estimate of the number of edges across the different days");
        log("UNION_ESTIMATE", "" + unionSizeEstimate);
        return null;
    }
}

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
package uk.gov.gchq.gaffer.example.gettingstarted.analytic;

import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator13;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import uk.gov.gchq.gaffer.user.User;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class LoadAndQuery13 extends LoadAndQuery {
    public LoadAndQuery13() {
        super("Using Union to estimate the size of the graph");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery13().run();
    }

    public Iterable<Entity> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();
        // ---------------------------------------------------------


        // [add] add the edges to the graph
        // ---------------------------------------------------------
        final Set<String> dummyData = Collections.singleton("");
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new DataGenerator13())
                        .objects(dummyData)
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
        final GetAllEntities get = new GetAllEntities();
        final Iterable<Entity> entities = graph.execute(get, user);
        for (final Entity entity : entities) {
            log("GET_ENTITIES", entity.toString());
        }
        // ---------------------------------------------------------


        // [get estimate separate days] Get the estimates out of the sketches for the separate days
        // ---------------------------------------------------------
        final GetAllEntities getAllEntities2 = new GetAllEntities();
        final Iterable<Entity> allEntities2 = graph.execute(getAllEntities2, user);
        final Iterator<Entity> it = allEntities2.iterator();
        final Entity entityDay1 = it.next();
        final CompactSketch sketchDay1 = ((Union) entityDay1.getProperty("size")).getResult();
        final Entity entityDay2 = it.next();
        final CompactSketch sketchDay2 = ((Union) entityDay2.getProperty("size")).getResult();
        final double estimateDay1 = sketchDay1.getEstimate();
        final double estimateDay2 = sketchDay2.getEstimate();
        // ---------------------------------------------------------
        log("\nThe estimates for the different days");
        log("GET_ESTIMATE_OVER_SEPARATE_DAYS", "" + estimateDay1);
        log("GET_ESTIMATE_OVER_SEPARATE_DAYS", "" + estimateDay2);


        // [get intersection] Get the number of edges in common across the two days
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
        final GetAllEntities getAllEntities = new GetAllEntities.Builder()
                .view(new View.Builder().entity("size", new ViewElementDefinition.Builder()
                        .groupBy() // set the group by properties to 'none'
                        .build())
                        .build())
                .build();
        final Iterable<Entity> allEntities = graph.execute(getAllEntities, user);
        final Entity entity = allEntities.iterator().next();
        final double unionSizeEstimate = ((Union) entity.getProperty("size")).getResult().getEstimate();
        // ---------------------------------------------------------
        log("\nThe estimate of the number of edges across the different days");
        log("UNION_ESTIMATE", "" + unionSizeEstimate);
        return null;
    }
}

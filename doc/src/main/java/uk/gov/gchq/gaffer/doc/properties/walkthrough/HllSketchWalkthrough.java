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

import com.yahoo.sketches.hll.HllSketch;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.doc.properties.generator.HllSketchElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;
import java.util.Set;

public class HllSketchWalkthrough extends PropertiesWalkthrough {
    public HllSketchWalkthrough() {
        super(HllSketch.class, "properties/hllSketch", HllSketchElementGenerator.class);
    }

    public static void main(final String[] args) throws OperationException {
        new HllSketchWalkthrough().run();
    }

    @Override
    public CloseableIterable<? extends Element> run() throws OperationException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "properties/hllSketch/schema"))
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
                        .generator(new HllSketchElementGenerator())
                        .input(dummyData)
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------
        log("Added 1000 entities for vertex A, each time with a HllSketch containing a vertex that A was seen in an edge with");


        // [get] Get all entities
        // ---------------------------------------------------------
        CloseableIterable<? extends Element> allEntities = graph.execute(new GetAllElements(), user);
        // ---------------------------------------------------------
        log("\nAll edges:");
        for (final Element entity : allEntities) {
            log("GET_ALL_ENTITIES_RESULT", entity.toString());
        }


        // [get the approximate degree of a] Get the entity for A and print out the estimate of the degree
        // ---------------------------------------------------------
        final GetElements query = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();
        final Element element;
        try (final CloseableIterable<? extends Element> elements = graph.execute(query, user)) {
            element = elements.iterator().next();
        }
        final HllSketch hllSketch = (HllSketch) element.getProperty("approxCardinality");
        final double approxDegree = hllSketch.getEstimate();
        final String degreeEstimate = "Entity A has approximate degree " + approxDegree;
        // ---------------------------------------------------------
        log("\nEntity A with an estimate of its degree");
        log("GET_APPROX_DEGREE_FOR_ENTITY_A", degreeEstimate);

        return null;
    }
}

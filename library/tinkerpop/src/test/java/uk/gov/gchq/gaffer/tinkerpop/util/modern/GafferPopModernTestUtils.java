/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.util.modern;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;

public final class GafferPopModernTestUtils {

    // Vertices
    public static final Person MARKO = new Person("1", "marko", 29);
    public static final Person VADAS = new Person("2", "vadas", 27);
    public static final Software LOP = new Software("3", "lop", "java");
    public static final Person JOSH = new Person("4", "josh", 32);
    public static final Software RIPPLE = new Software("5", "ripple", "java");
    public static final Person PETER = new Person("6", "peter", 35);

    static {
        MARKO.setKnows(new Pair<>(JOSH, 1.0), new Pair<>(VADAS, 0.5));
        MARKO.setCreated(new Pair<>(LOP, 0.4));
        JOSH.setCreated(new Pair(LOP, 0.4), new Pair(RIPPLE, 1.0));
        PETER.setCreated(new Pair(LOP, 0.2));
    }

    // Vertex labels/props
    public static final String PERSON = "person";
    public static final String SOFTWARE = "software";
    public static final String NAME = "name";
    public static final String AGE = "age";
    public static final String LANG = "lang";

    // Edge labels/props
    public static final String KNOWS = "knows";
    public static final String CREATED = "created";
    public static final String WEIGHT = "weight";

    public static final Configuration MODERN_CONFIGURATION = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.GRAPH, GafferPopGraph.class.getName());
            this.setProperty(GafferPopGraph.GRAPH_ID, "modern");
            this.setProperty(GafferPopGraph.USER_ID, "user01");
            // So we can add vertices for testing
            this.setProperty(GafferPopGraph.NOT_READ_ONLY_ELEMENTS, true);
        }
    };

    private GafferPopModernTestUtils() {
    }

    /*
     * Creates a graph of the Tinkerpop Modern dataset
     */
    public static GafferPopGraph createModernGraph(Class<?> clazz, StoreType storeType) {
        Graph g = GafferPopTestUtil.getGafferGraph(clazz, GafferPopTestUtil.getStoreProperties(storeType));
        GafferPopGraph graph = GafferPopGraph.open(MODERN_CONFIGURATION, g);

        addVertex(graph, MARKO);
        addVertex(graph, VADAS);
        addVertex(graph, LOP);
        addVertex(graph, JOSH);
        addVertex(graph, RIPPLE);
        addVertex(graph, PETER);

        addEdges(graph, MARKO);
        addEdges(graph, JOSH);
        addEdges(graph, PETER);

        return graph;
    }

    public static Vertex addVertex(GafferPopGraph graph, Person person) {
        return graph.addVertex(T.label, PERSON, T.id, person.getId(), NAME, person.getName(), AGE, person.getAge());
    }

    public static Vertex addVertex(GafferPopGraph graph, Software software) {
        return graph.addVertex(T.label, SOFTWARE, T.id, software.getId(), NAME, software.getName(), LANG,
                software.getLang());
    }

    public static void addEdges(GafferPopGraph graph, Person person) {
        addKnowsEdges(graph, person);
        addCreatedEdges(graph, person);
    }

    public static void addKnowsEdges(GafferPopGraph graph, Person person) {
        for (Pair<Person, Double> pair : person.getKnows()) {
            GafferPopEdge edge = new GafferPopEdge(KNOWS, person.getId(), pair.getFirst().getId(), graph);
            edge.property(WEIGHT, pair.getSecond());
            graph.addEdge(edge);
        }
    }

    public static void addCreatedEdges(GafferPopGraph graph, Person person) {
        for (Pair<Software, Double> pair : person.getCreated()) {
            GafferPopEdge edge = new GafferPopEdge(CREATED, person.getId(), pair.getFirst().getId(), graph);
            edge.property(WEIGHT, pair.getSecond());
            graph.addEdge(edge);
        }
    }
}

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

package uk.gov.gchq.gaffer.tinkerpop.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

public class GafferPopModernTestUtils {

    public static final Person MARKO = new Person("1", "marko", 29);
    public static final Person VADAS = new Person("2", "vadas", 27);
    public static final Software LOP = new Software("3", "lop", "java");
    public static final Person JOSH = new Person("4", "josh", 32);
    public static final Software RIPPLE = new Software("5", "ripple", "java");
    public static final Person PETER = new Person("6", "peter", 35);

    public static final String PERSON = "person";
    public static final String SOFTWARE = "software";
    public static final String NAME = "name";
    public static final String AGE = "age";
    public static final String LANG = "lang";

    public static final String KNOWS = "knows";
    public static final String CREATED = "created";
    public static final String WEIGHT = "weight";

    public static GafferPopGraph getModernGraph(Class<?> clazz, StoreProperties properties,
            Configuration configuration) {
        Graph g = GafferPopTestUtil.getGafferGraph(clazz, properties);
        GafferPopGraph graph = GafferPopGraph.open(configuration, g);

        Vertex marko = addVertex(graph, MARKO);
        Vertex vadas = addVertex(graph, VADAS);
        Vertex lop = addVertex(graph, LOP);
        Vertex josh = addVertex(graph, JOSH);
        Vertex ripple = addVertex(graph, RIPPLE);
        Vertex peter = addVertex(graph, PETER);

        GafferPopEdge markoKnowsVadas = new GafferPopEdge(KNOWS, marko, vadas, graph);
        markoKnowsVadas.property(WEIGHT, 0.5);
        graph.addEdge(markoKnowsVadas);

        GafferPopEdge markoKnowsJosh = new GafferPopEdge(KNOWS, marko, josh, graph);
        markoKnowsVadas.property(WEIGHT, 1.0);
        graph.addEdge(markoKnowsJosh);

        GafferPopEdge markoCreatedLop = new GafferPopEdge(CREATED, marko, lop, graph);
        markoCreatedLop.property(WEIGHT, 0.4);
        graph.addEdge(markoCreatedLop);

        GafferPopEdge joshCreatedRipple = new GafferPopEdge(CREATED, josh, ripple, graph);
        joshCreatedRipple.property(WEIGHT, 1.0);
        graph.addEdge(joshCreatedRipple);

        GafferPopEdge joshCreatedLop = new GafferPopEdge(CREATED, josh, lop, graph);
        joshCreatedLop.property(WEIGHT, 0.4);
        graph.addEdge(joshCreatedLop);

        GafferPopEdge peterCreatedLop = new GafferPopEdge(CREATED, peter, lop, graph);
        peterCreatedLop.property(WEIGHT, 0.2);
        graph.addEdge(peterCreatedLop);

        return graph;
    }

    private static Vertex addVertex(GafferPopGraph graph, Person person) {
        return graph.addVertex(T.label, PERSON, T.id, person.getId(), NAME, person.getName(), AGE, person.getAge());
    }

    private static Vertex addVertex(GafferPopGraph graph, Software software) {
        return graph.addVertex(T.label, SOFTWARE, T.id, software.getId(), NAME, software.getName(), LANG,
                software.getLang());
    }

    public static class Person {
        private final String id;
        private final String name;
        private final int age;

        public Person(String id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

    }

    public static class Software {
        private final String id;
        private final String name;
        private final String lang;

        public Software(String id, String name, String lang) {
            this.id = id;
            this.name = name;
            this.lang = lang;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getLang() {
            return lang;
        }
    }
}

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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    public static GafferPopGraph createModernGraph(Class<?> clazz, StoreProperties properties,
            Configuration configuration) {
        Graph g = GafferPopTestUtil.getGafferGraph(clazz, properties);
        GafferPopGraph graph = GafferPopGraph.open(configuration, g);

        Map<Object, Vertex> verticesById = new HashMap<>();

        Vertex marko = addVertex(graph, MARKO);
        verticesById.put(marko.id(), marko);
        Vertex vadas = addVertex(graph, VADAS);
        verticesById.put(vadas.id(), vadas);
        Vertex lop = addVertex(graph, LOP);
        verticesById.put(lop.id(), lop);
        Vertex josh = addVertex(graph, JOSH);
        verticesById.put(josh.id(), josh);
        Vertex ripple = addVertex(graph, RIPPLE);
        verticesById.put(ripple.id(), ripple);
        Vertex peter = addVertex(graph, PETER);
        verticesById.put(peter.id(), peter);

        addEdges(graph, MARKO, verticesById);
        addEdges(graph, JOSH, verticesById);
        addEdges(graph, PETER, verticesById);

        return graph;
    }

    private static Vertex addVertex(GafferPopGraph graph, Person person) {
        return graph.addVertex(T.label, PERSON, T.id, person.getId(), NAME, person.getName(), AGE, person.getAge());
    }

    private static Vertex addVertex(GafferPopGraph graph, Software software) {
        return graph.addVertex(T.label, SOFTWARE, T.id, software.getId(), NAME, software.getName(), LANG,
                software.getLang());
    }

    private static void addEdges(GafferPopGraph graph, Person person, Map<Object, Vertex> verticesById) {
        Vertex source = verticesById.get(person.getId());
        for (Pair<Person, Double> pair : person.getKnows()) {
            Vertex target = verticesById.get(pair.getFirst().getId());
            GafferPopEdge edge = new GafferPopEdge(KNOWS, source, target, graph);
            edge.property(WEIGHT, pair.getSecond());
            graph.addEdge(edge);
        }

        for (Pair<Software, Double> pair : person.getCreated()) {
            Vertex target = verticesById.get(pair.getFirst().getId());
            GafferPopEdge edge = new GafferPopEdge(CREATED, source, target, graph);
            edge.property(WEIGHT, pair.getSecond());
            graph.addEdge(edge);
        }
    }

    /**
     * Inner class to make it easy to reference 'person' vertices in tests
     */
    public static class Person {
        private final String id;
        private final String name;
        private final int age;
        private final Map<String, Object> propertyMap = new HashMap<>();
        private final Set<Pair<Person, Double>> knows = new HashSet<>();
        private final Set<Pair<Software, Double>> created = new HashSet<>();

        public Person(String id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
            propertyMap.put(NAME, Arrays.asList(name));
            propertyMap.put(AGE, Arrays.asList(age));
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

        public void setKnows(Pair<Person, Double>... people) {
            Collections.addAll(knows, people);
        }

        public Set<Pair<Person, Double>> getKnows() {
            return knows;
        }

        /**
         * Gets a list of EdgeIds representing all the 'knows' edges
         * from this person
         * Use for test comparisons
         *
         * @return Returns a list of EdgeIds
         */
        public List<List<String>> knowsEdges() {
            return knows.stream()
                .map(e -> Arrays.asList(this.getId(), e.getFirst().getId()))
                .collect(Collectors.toList());
        }

        /**
         * Helper method to create an EdgeId from this person to another
         * Note. This edge may not exist in the graph
         * @param otherPerson the target of the edge
         * @return Returns an EdgeId
         */
        public List<String> knows(Person otherPerson) {
            return Arrays.asList(this.getId(), otherPerson.getId());
        }

        public void setCreated(Pair<Software, Double> ... software) {
            Collections.addAll(created, software);
        }

        public Set<Pair<Software, Double>> getCreated() {
            return created;
        }

        /**
         * Gets a list of EdgeIds representing all the 'created' edges
         * from this person
         * Use for test comparisons
         *
         * @return Returns a list of EdgeIds
         */
        public List<List<String>> createdEdges() {
            return created.stream()
                .map(e -> Arrays.asList(this.getId(), e.getFirst().getId()))
                .collect(Collectors.toList());
        }

        /**
         * Helper method to create an EdgeId from this person to the software
         * Note. This edge may not exist in the graph
         * @param software the target of the edge
         * @return Returns an EdgeId
         */
        public List<String> created(Software software) {
            return Arrays.asList(this.getId(), software.getId());
        }


        /**
         * Gets a Map representation of the Vertex's properties
         *
         * @return map of properties
         */
        public Map<String, Object> getPropertyMap() {
            return propertyMap;
        }
    }

    /**
     * Inner class to make it easy to reference 'software' vertices in tests
     */
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

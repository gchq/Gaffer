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

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.generator.GafferPopVertexGenerator;

import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.AGE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PERSON;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helper class to make it easy to reference 'person' vertices in tests
 */
public class Person {
    private final String id;
    private final String name;
    private final int age;
    private final Map<Object, Object> propertyMap = new HashMap<>();
    private final Map<Object, Object> cypherMap = new HashMap<>();

    private final Set<Pair<Person, Double>> knows = new HashSet<>();
    private final Set<Pair<Software, Double>> created = new HashSet<>();

    public Person(String id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
        propertyMap.put(T.id, id);
        propertyMap.put(T.label, PERSON);
        propertyMap.put(GafferPopModernTestUtils.NAME, name);
        propertyMap.put(GafferPopModernTestUtils.AGE, age);
        cypherMap.put(T.id, id);
        cypherMap.put(T.label, PERSON);
        cypherMap.put(GafferPopModernTestUtils.NAME, Arrays.asList(name));
        cypherMap.put(GafferPopModernTestUtils.AGE, Arrays.asList(age));

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
            .map(e -> Arrays.asList(id, GafferPopModernTestUtils.KNOWS, e.getFirst().getId()))
            .collect(Collectors.toList());
    }

    /**
     * Helper method to create an EdgeId from this person to another
     * Note. This edge may not exist in the graph
     * @param otherPerson the target of the edge
     * @return Returns an EdgeId
     */
    public List<String> knows(Person otherPerson) {
        return Arrays.asList(id, GafferPopModernTestUtils.KNOWS, otherPerson.getId());
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
            .map(e -> Arrays.asList(id, e.getFirst().getId()))
            .collect(Collectors.toList());
    }

    /**
     * Helper method to create an EdgeId from this person to the software
     * Note. This edge may not exist in the graph
     * @param software the target of the edge
     * @return Returns an EdgeId
     */
    public List<String> created(Software software) {
        return Arrays.asList(id, GafferPopModernTestUtils.CREATED, software.getId());
    }


    /**
     * Gets a Map representation of the Vertex's properties
     *
     * @return map of properties
     */
    public Map<Object, Object> getPropertyMap() {
        return propertyMap;
    }

    /**
     * Gets a Cypher Map representation of the Vertex's properties
     *
     * @return map of properties
     */
    public Map<Object, Object> getCypherPropertyMap() {
        return cypherMap;
    }

    public Entity toEntity() {
        return new Entity.Builder()
                .group(PERSON)
                .vertex(id)
                .property(NAME, name)
                .property(AGE, age)
                .build();
    }

    public Vertex toVertex(GafferPopGraph graph) {
        return new GafferPopVertexGenerator(graph)._apply(toEntity());
    }
}

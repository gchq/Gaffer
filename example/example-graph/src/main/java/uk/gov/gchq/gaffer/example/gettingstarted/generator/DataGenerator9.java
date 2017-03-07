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

package uk.gov.gchq.gaffer.example.gettingstarted.generator;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import java.util.Arrays;

public class DataGenerator9 extends OneToManyElementGenerator<String> {
    @Override
    public Iterable<Element> getElements(final String line) {
        final String[] t = line.split(",");
        final Edge edge = new Edge.Builder()
                .group(t[0])
                .source(t[1]).dest(t[2]).directed(false)
                .property("count", 1)
                .build();

        final Entity sourceCardinality = createCardinalityEntity(
                edge.getSource(), edge.getDestination(), edge);
        final Entity destinationCardinality = createCardinalityEntity(
                edge.getDestination(), edge.getSource(), edge);
        return Arrays.asList(edge, sourceCardinality, destinationCardinality);
    }

    private Entity createCardinalityEntity(final Object source,
                                           final Object destination,
                                           final Edge edge) {
        final HyperLogLogPlus hllp = new HyperLogLogPlus(5, 5);
        hllp.offer(destination);

        return new Entity.Builder()
                .vertex(source)
                .group("Cardinality")
                .property("edgeGroup", CollectionUtil.treeSet(edge.getGroup()))
                .property("hllp", hllp)
                .property("count", 1)
                .build();
    }

    @Override
    public Iterable<String> getObjects(final Iterable<Element> elements) {
        throw new UnsupportedOperationException();
    }
}

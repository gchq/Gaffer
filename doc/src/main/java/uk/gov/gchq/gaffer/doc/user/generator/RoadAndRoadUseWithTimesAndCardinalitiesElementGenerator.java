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

package uk.gov.gchq.gaffer.doc.user.generator;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.commons.lang.time.DateUtils;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator implements OneToManyElementGenerator<String> {
    @Override
    public Iterable<Element> _apply(final String line) {
        final List<Edge> edges = createEdges(line);
        final List<Entity> cardinalities = createCardinalities(edges);

        // Create an iterable containing all the edges and cardinality entities
        return new ChainedIterable<>(edges, cardinalities);
    }

    private List<Edge> createEdges(final String line) {
        final String[] t = line.split(",");

        final String road = t[0];
        final String junctionA = t[1];
        final String junctionB = t[2];
        final Date timestamp;
        try {
            timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(t[3]);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid date: " + t[3]);
        }

        final Date startDate = DateUtils.truncate(timestamp, Calendar.DAY_OF_MONTH);
        final Date endDate = DateUtils.addMilliseconds(DateUtils.addDays(startDate, 1), -1);

        return Arrays.asList(
                new Edge.Builder()
                        .group("RoadHasJunction")
                        .source(road)
                        .dest(junctionA)
                        .directed(true)
                        .build(),

                new Edge.Builder()
                        .group("RoadHasJunction")
                        .source(road)
                        .dest(junctionB)
                        .directed(true)
                        .build(),

                new Edge.Builder()
                        .group("RoadUse")
                        .source(junctionA)
                        .dest(junctionB)
                        .directed(true)
                        .property("count", 1L)
                        .property("startDate", startDate)
                        .property("endDate", endDate)
                        .build()
        );
    }

    private List<Entity> createCardinalities(final List<Edge> edges) {
        final List<Entity> cardinalities = new ArrayList<>(edges.size() * 2);

        for (final Edge edge : edges) {
            cardinalities.add(createCardinality(edge.getSource(), edge.getDestination(), edge));
            cardinalities.add(createCardinality(edge.getDestination(), edge.getSource(), edge));
        }

        return cardinalities;
    }

    private Entity createCardinality(final Object source,
                                     final Object destination,
                                     final Edge edge) {
        final HyperLogLogPlus hllp = new HyperLogLogPlus(5, 5);
        hllp.offer(destination);

        return new Entity.Builder()
                .vertex(source)
                .group("Cardinality")
                .property("edgeGroup", CollectionUtil.treeSet(edge.getGroup()))
                .property("hllp", hllp)
                .property("count", 1L)
                .build();
    }
}

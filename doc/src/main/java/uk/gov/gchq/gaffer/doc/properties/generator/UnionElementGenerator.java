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
package uk.gov.gchq.gaffer.doc.properties.generator;

import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class UnionElementGenerator implements OneToManyElementGenerator<String> {
    public static Date getDate(final String dateStr) {
        try {
            return new SimpleDateFormat("dd/MM/yy").parse(dateStr);
        } catch (final ParseException e) {
            throw new IllegalArgumentException("Unable to parse date", e);
        }
    }

    @Override
    public Iterable<Element> _apply(final String line) {
        final Set<Element> elements = new HashSet<>();
        // On day 10/1/17 there are 1000 edges A-B0, A-B1, ..., A-B999.
        // For each edge we create an Entity with a union sketch containing the source and destination from the edge
        final Date midnight9th = getDate("09/01/17");
        final Date midnight10th = getDate("10/01/17");
        for (int i = 0; i < 1000; i++) {
            final Edge edge = new Edge.Builder()
                    .group("red")
                    .source("A")
                    .dest("B" + i)
                    .property("startDate", midnight9th)
                    .property("endDate", midnight10th)
                    .property("count", 1L)
                    .build();
            elements.add(edge);
            final Union union = Sketches.setOperationBuilder().buildUnion();
            union.update("A-B" + i);
            final Entity entity = new Entity.Builder()
                    .group("size")
                    .vertex("graph")
                    .property("startDate", midnight9th)
                    .property("endDate", midnight10th)
                    .property("size", union)
                    .build();
            elements.add(entity);
        }
        // On day 11/1/17 there are 500 edges A-B750, A-B751, ..., A-B1249.
        final Date midnight11th = getDate("11/01/17");
        for (int i = 750; i < 1250; i++) {
            final Edge edge = new Edge.Builder()
                    .group("red")
                    .source("A")
                    .dest("B" + i)
                    .property("startDate", midnight10th)
                    .property("endDate", midnight11th)
                    .property("count", 1L)
                    .build();
            elements.add(edge);
            final Union union = Sketches.setOperationBuilder().buildUnion();
            union.update("A-B" + i);
            final Entity entity = new Entity.Builder()
                    .group("size")
                    .vertex("graph")
                    .property("startDate", midnight10th)
                    .property("endDate", midnight11th)
                    .property("size", union)
                    .build();
            elements.add(entity);
        }
        return elements;
    }
}

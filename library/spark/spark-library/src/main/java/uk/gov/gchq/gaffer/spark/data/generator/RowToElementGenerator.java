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

package uk.gov.gchq.gaffer.spark.data.generator;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * A {@link OneToOneElementGenerator} for converting a {@link Row} object into a
 * Gaffer {@link Element}.
 *
 * This generator requires that the Row object to be converted into an Element was
 * originally created from an Element. It is not possible to convert an arbitrary
 * Row object into an Element.
 */
public class RowToElementGenerator implements OneToOneElementGenerator<Row> {

    private final List<String> reserved = Lists.newArrayList("src", "dst", "vertex", "directed", "group", "id", "matchedVertex");

    @Override
    public Element _apply(final Row row) {

        if (null == row.getAs("src")) {
            final String group = row.getAs("group");
            final Object vertex = row.getAs("id");

            final Entity.Builder builder = new Entity.Builder()
                    .group(group)
                    .vertex(vertex);

            filterProperties(row).forEach(n -> {
                final Object val = row.getAs(n);
                if (val instanceof String && Strings.isNullOrEmpty((String) val)) {
                    return;
                }
                builder.property(n, val);
            });

            return builder.build();
        } else {
            final String group = row.getAs("group");
            final boolean directed = row.getAs("directed");
            final String matchedVertex = row.getAs("matchedVertex");

            final Object source = row.getAs("src");
            final Object destination = row.getAs("dst");

            final Edge.Builder builder = new Edge.Builder()
                    .group(group)
                    .source(source)
                    .dest(destination)
                    .directed(directed);

            if (null != matchedVertex) {
                builder.matchedVertex(EdgeId.MatchedVertex.valueOf(matchedVertex));
            }

            filterProperties(row).forEach(n -> {
                final Object val = row.getAs(n);
                if (val instanceof String && Strings.isNullOrEmpty((String) val)) {
                    return;
                }
                builder.property(n, val);
            });

            return builder.build();
        }
    }

    private Stream<String> filterProperties(final Row row) {
        return Arrays.stream(row.schema().fieldNames())
                .filter(n -> !reserved.contains(n))
                .filter(n -> !row.isNullAt(row.fieldIndex(n)));
    }
}

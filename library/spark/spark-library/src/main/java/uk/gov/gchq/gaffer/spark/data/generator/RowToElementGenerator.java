/*
 * Copyright 2017-2018 Crown Copyright
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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Row;

import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.Validator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.ReservedPropertyNames;
import uk.gov.gchq.gaffer.data.element.id.EdgeId.MatchedVertex;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema.SchemaToStructTypeConverter;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A {@code RowToElementGenerator} is a {@link OneToOneElementGenerator} for
 * converting a {@link Row} objects into a Gaffer {@link Element}.
 * This generator requires that the Row object to be converted into an Element was
 * originally created from an Element. It is not possible to convert an arbitrary
 * Row object into an Element.
 */
public class RowToElementGenerator implements OneToOneElementGenerator<Row> {
    private final Validator<Row> rowValidator = r -> null != r && null != r.getAs(SchemaToStructTypeConverter.GROUP);

    @Override
    public Iterable<? extends Element> apply(final Iterable<? extends Row> rows) {
        return new TransformIterable<Row, Element>(rows, rowValidator, true) {
            @Override
            protected Element transform(final Row row) {
                return _apply(row);
            }
        };
    }

    @Override
    public Element _apply(final Row row) {
        final Element element;
        final String group = row.getAs(SchemaToStructTypeConverter.GROUP);
        final Object source = ArrayUtils.contains(row.schema().fieldNames(), SchemaToStructTypeConverter.SRC_COL_NAME) ? row.getAs(SchemaToStructTypeConverter.SRC_COL_NAME) : null;
        if (null != source) {
            final Object destination = row.getAs(SchemaToStructTypeConverter.DST_COL_NAME);
            final boolean directed = row.getAs(SchemaToStructTypeConverter.DIRECTED_COL_NAME);
            final MatchedVertex matchedVertex;
            if (ArrayUtils.contains(row.schema().fieldNames(), SchemaToStructTypeConverter.MATCHED_VERTEX_COL_NAME)) {
                final String matchedVertexStr = row.getAs(SchemaToStructTypeConverter.MATCHED_VERTEX_COL_NAME);
                matchedVertex = null != matchedVertexStr ? MatchedVertex.valueOf(matchedVertexStr) : null;
            } else {
                matchedVertex = null;
            }
            element = new Edge(group, source, destination, directed, matchedVertex, null);
        } else {
            final Object vertex = ArrayUtils.contains(row.schema().fieldNames(), SchemaToStructTypeConverter.VERTEX_COL_NAME) ? row.getAs(SchemaToStructTypeConverter.VERTEX_COL_NAME) : row.getAs(SchemaToStructTypeConverter.ID);
            element = new Entity(group, vertex);
        }

        getPropertyNames(row).forEach(n -> {
            element.putProperty(n, row.getAs(n));
        });

        return element;
    }

    private Stream<String> getPropertyNames(final Row row) {
        return Arrays.stream(row.schema().fieldNames())
                .filter(f -> !ReservedPropertyNames.contains(f))
                .filter(n -> !row.isNullAt(row.fieldIndex(n)));
    }
}

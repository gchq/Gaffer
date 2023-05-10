/*
 * Copyright 2022-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import uk.gov.gchq.gaffer.commonutil.PropertiesUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition;
import uk.gov.gchq.gaffer.data.element.function.TuplesToElements;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.function.CsvLinesToMaps;
import uk.gov.gchq.koryphe.impl.function.FunctionChain;
import uk.gov.gchq.koryphe.impl.function.IterableFunction;
import uk.gov.gchq.koryphe.impl.function.MapToTuple;
import uk.gov.gchq.koryphe.impl.function.ParseTime;
import uk.gov.gchq.koryphe.impl.function.ToBoolean;
import uk.gov.gchq.koryphe.impl.function.ToDouble;
import uk.gov.gchq.koryphe.impl.function.ToFloat;
import uk.gov.gchq.koryphe.impl.function.ToInteger;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.function.ToString;
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Since("2.0.0")
@Summary("Generates elements from an OpenCypher CSV")
public abstract class OpenCypherCsvElementGenerator implements ElementGenerator<String> {
    private int firstRow = 1;
    private Boolean trim = false;
    private char delimiter = ',';
    private String nullString = "";

    // Map of Identifier name to header value
    protected LinkedHashMap<String, String> fields = getFields();

    protected abstract LinkedHashMap<String, String> getFields();

    @Override
    public Iterable<? extends Element> apply(final Iterable<? extends String> strings) {
        final String header = strings.iterator().next();

        // Transform each line into a Map
        final CsvLinesToMaps parseCsv = new CsvLinesToMaps()
                .firstRow(firstRow)
                .trim(trim)
                .nullString(nullString)
                .delimiter(delimiter)
                .parseHeader(header);

        // Turn the Map into a Tuple
        final IterableFunction<Map<String, Object>, Tuple<String>> toTuples = new IterableFunction<>(new MapToTuple<String>());

        // FunctionChain to apply to each line will be built by looking at property types and adding functions from type mapping
        FunctionChain.Builder<Tuple<String>, Tuple<String>> transformTuplesBuilder = new FunctionChain.Builder<>();

        // ElementTupleDefinitions created from identifiers
        // Each property will be added into the ElementTupleDefinitions too
        ElementTupleDefinition entityDefinition = new ElementTupleDefinition(fields.get(CsvGenerator.ENTITY_GROUP)).vertex(fields.get(IdentifierType.VERTEX.name()));
        ElementTupleDefinition edgeDefinition = new ElementTupleDefinition(fields.get(CsvGenerator.EDGE_GROUP))
                .source(fields.get(IdentifierType.SOURCE.name()))
                .destination(fields.get(IdentifierType.DESTINATION.name()))
                .property("edge-id", fields.get(IdentifierType.VERTEX.name()));

        for (final String columnHeader : parseCsv.getHeader()) {
            if (!fields.values().contains(columnHeader)) {
                final String propertyName = PropertiesUtil.stripInvalidCharacters(columnHeader.split(":")[0].replaceAll("_", "-"));
                if (columnHeader.contains(":")) {
                    final String typeName = columnHeader.split(":")[1];
                    final KorypheFunction<?, ?> transform = TRANSFORM_MAPPINGS.getOrDefault(typeName, new ToString());
                    transformTuplesBuilder = transformTuplesBuilder.execute(new String[]{columnHeader}, transform, new String[]{propertyName});
                }
                entityDefinition = entityDefinition.property(propertyName);
                edgeDefinition = edgeDefinition.property(propertyName);
            }
        }

        // TransformTuples can be built now
        final IterableFunction<Tuple<String>, Tuple<String>> transformTuples = new IterableFunction(transformTuplesBuilder.build());

        // TuplesToElements created with updated ElementTupleDefinitions
        final TuplesToElements toElements = new TuplesToElements()
                .element(entityDefinition)
                .element(edgeDefinition)
                .useGroupMapping(true);

        // Apply all functions in order to each line
        final FunctionChain<Iterable<String>, Iterable<Element>> generator = new FunctionChain.Builder<Iterable<String>, Iterable<Element>>()
                .execute(parseCsv)
                .execute(toTuples)
                .execute(transformTuples)
                .execute(toElements)
                .build();

        return generator.apply((Iterable<String>) strings);
    }

    // Map of csv type name to relevant Koryphe transform function
    public static final Map<String, KorypheFunction<?, ?>> TRANSFORM_MAPPINGS  = Collections.unmodifiableMap(new HashMap<String, KorypheFunction<?, ?>>() { {
        put("String", new ToString());
        put("Char", new ToString());
        put("Date", new ToString());
        put("LocalDate", new ToString());
        put("LocalDateTime", new ToString());
        put("Point", new ToString());
        put("Duration", new ToString());
        put("Int", new ToInteger());
        put("Short", new ToInteger());
        put("Byte", new ToInteger());
        put("DateTime", new ParseTime());
        put("Long", new ToLong());
        put("Double", new ToDouble());
        put("Float", new ToFloat());
        put("Boolean", new ToBoolean());
    } });

    public int getFirstRow() {
        return firstRow;
    }

    public void setFirstRow(final int firstRow) {
        this.firstRow = firstRow;
    }

    public Boolean getTrim() {
        return trim;
    }

    public void setTrim(final Boolean trim) {
        this.trim = trim;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(final char delimiter) {
        this.delimiter = delimiter;
    }

    public String getNullString() {
        return nullString;
    }

    public void setNullString(final String nullString) {
        this.nullString = nullString;
    }
}

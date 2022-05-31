/*
 * Copyright 2022 Crown Copyright
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

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.PropertiesUtil;
import uk.gov.gchq.gaffer.data.element.Element;
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

import java.io.Serializable;
import java.util.List;
import java.util.Map;


@Since("2.0.0")
@Summary("Generates elements from an openCypher CSV string")
public class OpenCypherCsvElementGenerator implements ElementGenerator<String>, Serializable {
    private static final long serialVersionUID = -821376598172364516L;
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenCypherCsvElementGenerator.class);

    public static final String VERTEX = ":ID";
    public static final String NEO4J_VERTEX = "_id";
    public static final String ENTITY_GROUP = ":LABEL";
    public static final String NEO4J_ENTITY_GROUP = "_labels";

    public static final String SOURCE = ":START_ID";
    public static final String NEO4J_SOURCE = "_start";
    public static final String DESTINATION = ":END_ID";
    public static final String NEO4J_DESTINATION = "_end";

    public static final String EDGE_GROUP = ":TYPE";
    public static final String NEO4J_EDGE_GROUP = "_type";
    private static final List<String> ELEMENT_COLUMN_NAMES = ImmutableList.of(VERTEX, ENTITY_GROUP, EDGE_GROUP, SOURCE, DESTINATION);
    private String header;
    private int firstRow = 0;
    private Boolean trim = false;
    private char delimiter = ',';
    private String nullString = "";

    @Override
    public Iterable<? extends Element> apply(final Iterable<? extends String> strings) {
        CsvLinesToMaps parseCsv = new CsvLinesToMaps()
            .firstRow(1)
            .trim(trim)
            .nullString(nullString)
            .delimiter(delimiter)
            .parseHeader(header);

        IterableFunction<Map<String, Object>, Tuple<String>> toTuples = new IterableFunction<>(new MapToTuple<String>());

        FunctionChain.Builder<Tuple<String>, Tuple<String>> transformTuplesBuilder = new FunctionChain.Builder<>();

        ElementTupleDefinition entityDefinition = new ElementTupleDefinition(ENTITY_GROUP).vertex(VERTEX);
        ElementTupleDefinition edgeDefinition = new ElementTupleDefinition(EDGE_GROUP)
            .source(SOURCE)
            .destination(DESTINATION)
            .property("edge-id", VERTEX);
        for (final String columnHeader : parseCsv.getHeader()) {
            if (!ELEMENT_COLUMN_NAMES.contains(columnHeader)) {
                String propertyName = columnHeader.split(":")[0];
                if (!PropertiesUtil.isValidName(propertyName)) {
                    propertyName = propertyName.replaceAll("_", "-");
                    propertyName = PropertiesUtil.stripInvalidCharacters(propertyName);
                }
                if (columnHeader.contains(":")) {
                    String typeName = columnHeader.split(":")[1];
                    KorypheFunction<?, ?> transform;
                    switch (typeName) {
                        case "DateTime":
                            transform = new ParseTime();
                            break;
                        case "Long":
                            transform = new ToLong();
                            break;
                        case "Byte":
                        case "Short":
                        case "Int":
                            transform = new ToInteger();
                            break;
                        case "Boolean":
                            transform = new ToBoolean();
                            break;
                        case "Float":
                            transform = new ToFloat();
                            break;
                        case "Double":
                            transform = new ToDouble();
                            break;
                        case "Char":
                        case "Date":
                        case "LocalDate":
                        case "LocalDateTime":
                        case "Duration":
                        case "Point":
                        case "String":
                            transform = new ToString();
                            break;
                        default:
                            throw new RuntimeException("Unsupported Type: " + typeName);
                    }
                    transformTuplesBuilder = transformTuplesBuilder.execute(new String[]{columnHeader}, transform, new String[]{propertyName});
                }

                entityDefinition = entityDefinition.property(propertyName);
                edgeDefinition = edgeDefinition.property(propertyName);
            }
        }

        IterableFunction<Tuple<String>, Tuple<String>> transformTuples = new IterableFunction(transformTuplesBuilder.build());

        TuplesToElements toElements = new TuplesToElements()
            .element(entityDefinition)
            .element(edgeDefinition)
            .useGroupMapping(true);
        // Apply functions
        final FunctionChain<Iterable<String>, Iterable<Element>> generator = new FunctionChain.Builder<Iterable<String>, Iterable<Element>>()
                .execute(parseCsv)
                .execute(toTuples)
                .execute(transformTuples)
                .execute(toElements)
                .build();
        return generator.apply((Iterable<String>) strings);
    }

    private String parseHeader(final String header) {
        String parsedHeader = header.replace(NEO4J_VERTEX, VERTEX);
        parsedHeader = parsedHeader.replace(NEO4J_ENTITY_GROUP, ENTITY_GROUP);
        parsedHeader = parsedHeader.replace(NEO4J_EDGE_GROUP, EDGE_GROUP);
        parsedHeader = parsedHeader.replace(NEO4J_SOURCE, SOURCE);
        parsedHeader = parsedHeader.replace(NEO4J_DESTINATION, DESTINATION);
        return parsedHeader;
    }

    public String getHeader() {
        return header;
    }


    public void setHeader(final String header) {
        this.header = parseHeader(header);
    }


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

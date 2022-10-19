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
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Since("2.0.0")
@Summary("Generates elements from an  CSV string")
public class CsvElementGenerator implements ElementGenerator<String>, Serializable {
    private static final long serialVersionUID = -821376598172364516L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvElementGenerator.class);

    private String header;
    private int firstRow = 0;
    private Boolean trim = false;
    private char delimiter = ',';
    private String nullString = "";
    private CsvFormat csvFormat;

    @Override
    public Iterable<? extends Element> apply(final Iterable<? extends String> strings) {
        List<String> elementColumnNames = new ArrayList<String>(CsvFormat.getIdentifiers(csvFormat).values());
        //List<String> elementColumnNames = ImmutableList.of(csvFormat.getVertex(), csvFormat.getEntityGroup(), csvFormat.getEdgeGroup(), csvFormat.getSource(), csvFormat.getDestination());
        CsvLinesToMaps parseCsv = new CsvLinesToMaps()
                .firstRow(1)
                .trim(trim)
                .nullString(nullString)
                .delimiter(delimiter)
                .parseHeader(header);

        IterableFunction<Map<String, Object>, Tuple<String>> toTuples = new IterableFunction<>(new MapToTuple<String>());

        FunctionChain.Builder<Tuple<String>, Tuple<String>> transformTuplesBuilder = new FunctionChain.Builder<>();

        ElementTupleDefinition entityDefinition = new ElementTupleDefinition(csvFormat.getEntityGroup()).vertex(csvFormat.getVertex());
        ElementTupleDefinition edgeDefinition = new ElementTupleDefinition(csvFormat.getEdgeGroup())
                .source(csvFormat.getSource())
                .destination(csvFormat.getDestination())
                .property("edge-id", csvFormat.getVertex());
        for (final String columnHeader : parseCsv.getHeader()) {
            if (!elementColumnNames.contains(columnHeader)) {
                String propertyName = columnHeader.split(":")[0];
                if (!PropertiesUtil.isValidName(propertyName)) {
                    propertyName = propertyName.replaceAll("_", "-");
                    propertyName = PropertiesUtil.stripInvalidCharacters(propertyName);
                }
                if (columnHeader.contains(":")) {
                    String typeName = columnHeader.split(":")[1];
                    HashMap<String, KorypheFunction<?, ?>> transforms = CsvFormat.getTransforms(csvFormat);
                    KorypheFunction<?, ?> transform;
                    if (transforms.containsKey(typeName)) {
                        transform = transforms.get(typeName);
                    } else {
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


    public String getHeader() {
        return header;
    }


    public void setHeader(final String header) {
        this.header = header;
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

    public CsvFormat getCsvFormat() {
        return csvFormat;
    }

    public void setCsvFormat(final CsvFormat csvFormat) {
        this.csvFormat = csvFormat;
    }

    public static class Builder {
        private String header;
        private int firstRow = 0;
        private Boolean trim = false;
        private char delimiter = ',';
        private String nullString = "";

        private CsvFormat csvFormat;


        public CsvElementGenerator.Builder header(final String header) {
            this.header = header;
            return this;
        }

        public CsvElementGenerator.Builder csvFormat(final CsvFormat csvFormat) {
            this.csvFormat = csvFormat;
            return this;
        }
        public CsvElementGenerator.Builder firstRow(final int firstRow) {
            this.firstRow = firstRow;
            return this;
        }
        public CsvElementGenerator.Builder trim(final boolean trim) {
            this.trim = trim;
            return this;
        }
        public CsvElementGenerator.Builder delimiter(final char delimiter) {
            this.delimiter = delimiter;
            return this;
        }
        public CsvElementGenerator.Builder nullString(final String nullString) {
            this.nullString = nullString;
            return this;
        }
        public CsvElementGenerator build() {
            CsvElementGenerator generator = new CsvElementGenerator();
            generator.setNullString(nullString);
            generator.setDelimiter(delimiter);
            generator.setHeader(header);
            generator.setTrim(trim);
            generator.setFirstRow(firstRow);
            generator.setCsvFormat(csvFormat);
            return generator;
        }
    }
}

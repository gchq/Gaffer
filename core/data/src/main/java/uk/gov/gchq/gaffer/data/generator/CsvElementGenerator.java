/*
 * Copyright 2016-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.StreamIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.Validator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.function.PropertiesFilter;
import uk.gov.gchq.gaffer.data.element.function.PropertiesTransformer;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

@Since("1.8.0")
@Summary("Generates elements form a CSV string")
@JsonPropertyOrder(value = {
        "header", "firstRow", "delimiter", "quoted", "quoteChar",
        "requiredFields", "allFieldsRequired",
        "csvValidator", "csvTransforms", "entities", "edges",
        "followOnGenerator", "elementValidator"},
        alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class CsvElementGenerator implements OneToManyElementGenerator<String> {
    private List<String> header = new ArrayList<>();
    private int firstRow = 0;
    private char delimiter = ',';
    private boolean quoted = false;
    private char quoteChar = '\"';
    private boolean skipInvalid = false;

    private boolean allFieldsRequired = false;
    private Collection<String> requiredFields = new HashSet<>();
    private PropertiesFilter csvValidator = new PropertiesFilter();
    private PropertiesTransformer transformer = new PropertiesTransformer();
    private final List<CsvElementDef> entities = new ArrayList<>();
    private final List<CsvElementDef> edges = new ArrayList<>();
    private ElementGenerator<Element> followOnGenerator;
    private ElementFilter elementValidator;

    @Override
    public Iterable<? extends Element> apply(final Iterable<? extends String> strings) {
        if (allFieldsRequired) {
            requiredFields = header;
        }

        final LimitedCloseableIterable<? extends String> csvWithAtFirstRow = new LimitedCloseableIterable<>(strings, firstRow, null);
        Iterable<? extends Element> elements = new TransformOneToManyIterable<String, Element>(csvWithAtFirstRow) {
            @Override
            protected Iterable<Element> transform(final String item) {
                return _apply(item);
            }
        };

        if (null != followOnGenerator) {
            elements = followOnGenerator.apply(elements);
        }

        if (null != elementValidator && null != elementValidator.getComponents()) {
            final Validator<Element> validator = element -> elementValidator.test(element);
            elements = new TransformIterable<Element, Element>(elements, validator, skipInvalid) {
                @Override
                protected Element transform(final Element element) {
                    return element;
                }
            };
        }

        return elements;
    }

    @Override
    public Iterable<Element> _apply(final String csv) {
        return generateElements(csv);
    }

    private StreamIterable<Element> generateElements(final String csv) {
        return new StreamIterable<>(() -> {
            final CSVRecord csvRecord = parseCsv(csv);
            final Properties properties = extractProperties(csvRecord);
            final ValidationResult requiredFieldsResult = new ValidationResult();
            for (final String key : requiredFields) {
                if (StringUtils.isEmpty((String) properties.get(key))) {
                    requiredFieldsResult.addError(key + " was missing");
                }
            }

            //TODO skipInvalid
            if (!requiredFieldsResult.isValid()) {
                throw new IllegalArgumentException("CSV is invalid: " + csv + "\n " + requiredFieldsResult.getErrorString());
            }
            if (!csvValidator.test(properties)) {
                final ValidationResult result = csvValidator.testWithValidationResult(properties);
                throw new IllegalArgumentException("CSV is invalid. " + csv + "\n " + result.getErrorString());
            }
            transformer.apply(properties);
            return Stream.concat(
                    entities.stream().map(e -> transformCsvToElement(properties, e, Entity::new)),
                    edges.stream().map(e -> transformCsvToElement(properties, e, Edge::new))
            );
        });
    }

    private Properties extractProperties(final CSVRecord csvRecord) {
        final Iterator<String> columnNamesItr = header.iterator();
        final Properties properties = new Properties();
        for (final String columnValue : csvRecord) {
            properties.put(columnNamesItr.next(), columnValue);
        }
        return properties;
    }

    private CSVRecord parseCsv(final String csv) {
        final CSVRecord csvRecord;
        try {
            csvRecord = new CSVParser(new StringReader(csv), getCsvFormat()).iterator().next();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        if (csvRecord.size() != header.size()) {
            throw new IllegalArgumentException(
                    "CSV has " + csvRecord.size()
                            + " columns, but there are " + header.size()
                            + " provided column names"
            );
        }
        return csvRecord;
    }

    private CSVFormat getCsvFormat() {
        CSVFormat format = CSVFormat.DEFAULT;
        if (quoted) {
            format = format.withQuote(quoteChar);
        }
        format.withDelimiter(delimiter);
        return format;
    }

    private Element transformCsvToElement(final Properties properties,
                                          final CsvElementDef csvElementDef,
                                          final Function<String, Element> elementSupplier) {
        final Element element = elementSupplier.apply(csvElementDef.getGroup());
        element.copyProperties(properties);

        final Element transformedElement = csvElementDef.getElementTransformer().apply(element);
        transformedElement.getProperties().remove(properties.keySet());

        return transformedElement;
    }

    public List<String> getHeader() {
        return header;
    }

    public void setHeader(final List<String> header) {
        this.header.clear();
        this.header.addAll(header);
    }

    public int getFirstRow() {
        return firstRow;
    }

    public void setFirstRow(final int firstRow) {
        this.firstRow = firstRow;
    }

    public CsvElementGenerator firstRow(final int firstRow) {
        this.firstRow = firstRow;
        return this;
    }

    public CsvElementGenerator header(final String... header) {
        Collections.addAll(this.header, header);
        return this;
    }

    public CsvElementGenerator header(final Collection<String> header) {
        this.header.addAll(header);
        return this;
    }

    public List<CsvElementDef> getEntities() {
        return entities;
    }

    public void setEntities(final List<CsvElementDef> entities) {
        this.entities.clear();
        this.entities.addAll(entities);
    }

    public CsvElementGenerator entity(final String group, final ElementTransformer elementTransformer) {
        entities.add(new CsvElementDef(group, elementTransformer));
        return this;
    }

    public List<CsvElementDef> getEdges() {
        return edges;
    }

    public void setEdges(final List<CsvElementDef> edges) {
        this.edges.clear();
        this.edges.addAll(edges);
    }

    public CsvElementGenerator edge(final String group, final ElementTransformer elementTransformer) {
        edges.add(new CsvElementDef(group, elementTransformer));
        return this;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Collection<String> getRequiredFields() {
        if (allFieldsRequired) {
            return null;
        }
        return requiredFields;
    }

    public void setRequiredFields(final Collection<String> requiredFields) {
        this.requiredFields = requiredFields;
    }

    public CsvElementGenerator requiredFields(final String... requiredFields) {
        Collections.addAll(this.requiredFields, requiredFields);
        return this;
    }

    public boolean isAllFieldsRequired() {
        return allFieldsRequired;
    }

    public void setAllFieldsRequired(final boolean allFieldsRequired) {
        this.allFieldsRequired = allFieldsRequired;
    }

    public CsvElementGenerator allFieldsRequired() {
        this.allFieldsRequired = true;
        return this;
    }

    public CsvElementGenerator allFieldsRequired(final boolean allFieldsRequired) {
        this.allFieldsRequired = allFieldsRequired;
        return this;
    }

    public PropertiesFilter getCsvValidator() {
        return csvValidator;
    }

    public void setCsvValidator(final PropertiesFilter validator) {
        this.csvValidator = validator;
    }

    public CsvElementGenerator csvValidator(final PropertiesFilter csvValidator) {
        requireNonNull(csvValidator, "csvValidator is required");
        this.csvValidator = csvValidator;
        return this;
    }

    public ElementFilter getElementValidator() {
        return elementValidator;
    }

    public void setElementValidator(final ElementFilter elementValidator) {
        this.elementValidator = elementValidator;
    }

    public void elementValidator(final ElementFilter elementValidator) {
        this.elementValidator = elementValidator;
    }

    public boolean isSkipInvalid() {
        return skipInvalid;
    }

    public void setSkipInvalid(final boolean skipInvalid) {
        this.skipInvalid = skipInvalid;
    }

    public CsvElementGenerator skipInvalid() {
        this.skipInvalid = true;
        return this;
    }

    public CsvElementGenerator skipInvalid(final boolean skipInvalid) {
        this.skipInvalid = skipInvalid;
        return this;
    }

    @JsonIgnore
    public PropertiesTransformer getTransformer() {
        return transformer;
    }

    @JsonIgnore
    public void setTransformer(final PropertiesTransformer transformer) {
        requireNonNull(transformer, "transformer is required");
        this.transformer = transformer;
    }

    public CsvElementGenerator transformer(final PropertiesTransformer transformer) {
        requireNonNull(transformer, "transformer is required");
        this.transformer = transformer;
        return this;
    }

    public List<TupleAdaptedFunction<String, ?, ?>> getCsvTransforms() {
        return null != transformer ? transformer.getComponents() : null;
    }

    public void setCsvTransforms(final List<TupleAdaptedFunction<String, ?, ?>> transformeFunctions) {
        requireNonNull(transformer, "transformer is required");
        this.transformer = new PropertiesTransformer();
        this.transformer.setComponents(transformeFunctions);
    }

    public char getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(final char delimiter) {
        this.delimiter = delimiter;
    }

    public CsvElementGenerator delimiter(final char delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public boolean isQuoted() {
        return quoted;
    }

    public void setQuoted(final boolean quoted) {
        this.quoted = quoted;
    }

    public CsvElementGenerator quoted() {
        this.quoted = true;
        return this;
    }

    public CsvElementGenerator quoted(final boolean quoted) {
        this.quoted = quoted;
        return this;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(final char quoteChar) {
        this.quoteChar = quoteChar;
    }

    public CsvElementGenerator quoteChar(final char quoteChar) {
        this.quoteChar = quoteChar;
        return this;
    }

    public ElementGenerator<Element> getFollowOnGenerator() {
        return followOnGenerator;
    }

    public void setFollowOnGenerator(final ElementGenerator<Element> followOnGenerator) {
        this.followOnGenerator = followOnGenerator;
    }

    public CsvElementGenerator followOnGenerator(final ElementGenerator<Element> followOnGenerator) {
        this.followOnGenerator = followOnGenerator;
        return this;
    }

    public static class CsvElementDef {
        private String group;
        private ElementTransformer elementTransformer;

        public CsvElementDef() {
        }

        public CsvElementDef(final String group, final ElementTransformer elementTransformer) {
            this.group = group;
            this.elementTransformer = elementTransformer;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(final String group) {
            this.group = group;
        }

        @JsonIgnore
        public ElementTransformer getElementTransformer() {
            return elementTransformer;
        }

        @JsonIgnore
        public void setElementTransformer(final ElementTransformer elementTransformer) {
            this.elementTransformer = elementTransformer;
        }

        @JsonGetter("functions")
        public List<TupleAdaptedFunction<String, ?, ?>> getFunctions() {
            return null != elementTransformer ? elementTransformer.getComponents() : null;
        }

        public void setFunctions(final List<TupleAdaptedFunction<String, ?, ?>> functions) {
            this.elementTransformer = new ElementTransformer();
            this.elementTransformer.setComponents(functions);
        }
    }
}

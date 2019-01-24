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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.gaffer.commonutil.iterable.StreamIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.Validator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.PropertiesFilter;
import uk.gov.gchq.gaffer.data.element.function.PropertiesTransformer;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Since("1.8.0")
@Summary("Generates elements from maps")
@JsonPropertyOrder(value = {
        "requiredFields", "allFieldsRequired",
        "mapValidator", "mapTransforms", "elements",
        "followOnGenerator", "elementValidator"},
        alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class MapElementGenerator implements OneToManyElementGenerator<Map<String, Object>>, Serializable {
    public static final String GROUP = "GROUP";
    public static final String VERTEX = "VERTEX";
    public static final String SOURCE = "SOURCE";
    public static final String DESTINATION = "DESTINATION";
    public static final String DIRECTED = "DIRECTED";

    private static final long serialVersionUID = 270083544996344124L;

    private boolean skipInvalid = false;

    private boolean allFieldsRequired = false;
    private Collection<String> requiredFields = new HashSet<>();
    private PropertiesFilter mapValidator;
    private PropertiesTransformer transformer = new PropertiesTransformer();
    private final List<MapElementDef> elements = new ArrayList<>();
    private ElementGenerator<Element> followOnGenerator;
    private ElementFilter elementValidator;

    @Override
    public Iterable<? extends Element> apply(final Iterable<? extends Map<String, Object>> maps) {
        Iterable<? extends Element> elements = new TransformOneToManyIterable<Map<String, Object>, Element>(maps) {
            @Override
            protected Iterable<Element> transform(final Map<String, Object> map) {
                return _apply(map);
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
    public Iterable<Element> _apply(final Map<String, Object> map) {
        return generateElements(map);
    }

    private StreamIterable<Element> generateElements(final Map<String, Object> map) {
        return new StreamIterable<>(() -> {
            final Properties properties = extractProperties(map);
            final ValidationResult requiredFieldsResult = new ValidationResult();
            for (final String key : requiredFields) {
                if (StringUtils.isEmpty((String) properties.get(key))) {
                    requiredFieldsResult.addError(key + " was missing");
                }
            }

            if (!skipInvalid) {
                if (!requiredFieldsResult.isValid()) {
                    throw new IllegalArgumentException("Map is invalid: " + map + "\n " + requiredFieldsResult.getErrorString());
                }
                if (null != mapValidator && null != mapValidator.getComponents() && !mapValidator.test(properties)) {
                    final ValidationResult result = mapValidator.testWithValidationResult(properties);
                    throw new IllegalArgumentException("Map is invalid. " + map + "\n " + result.getErrorString());
                }
            }
            transformer.apply(properties);
            return elements.stream().map(e -> transformMapToElement(properties, e));
        });
    }

    private Properties extractProperties(final Map<String, Object> map) {
        final Properties properties = new Properties();
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    private Element transformMapToElement(final Properties properties,
                                          final MapElementDef mapElementDef) {
        requireNonNull(mapElementDef.get(GROUP), "GROUP is required");
        final Element element;
        if (mapElementDef.containsKey("VERTEX")) {
            element = new Entity(mapElementDef.getGroup(), getField(VERTEX, mapElementDef, properties));
        } else {
            element = new Edge(
                    mapElementDef.getGroup(),
                    getField(SOURCE, mapElementDef, properties),
                    getField(DESTINATION, mapElementDef, properties),
                    (boolean) getField(DIRECTED, mapElementDef, properties)
            );
        }

        for (final Map.Entry<String, Object> entry : mapElementDef.entrySet()) {
            final IdentifierType id = IdentifierType.fromName(entry.getKey());
            if (null == id) {
                element.putProperty(entry.getKey(), getField(entry.getValue(), properties));
            }
        }

        return element;
    }

    private Object getField(final String key, final MapElementDef mapElementDef, final Properties properties) {
        return getField(mapElementDef.get(key), properties);
    }

    private Object getField(final Object value, final Properties properties) {
        if (null == value) {
            return null;
        }

        if (value instanceof String) {
            final Object propValue = properties.get(value);
            if (null != propValue) {
                return propValue;
            }
        }
        return value;
    }

    public List<MapElementDef> getElements() {
        return elements;
    }

    public void setElements(final List<MapElementDef> elements) {
        this.elements.clear();
        this.elements.addAll(elements);
    }

    public MapElementGenerator element(final MapElementDef elementDef) {
        elements.add(elementDef);
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

    public MapElementGenerator requiredFields(final String... requiredFields) {
        Collections.addAll(this.requiredFields, requiredFields);
        return this;
    }

    public boolean isAllFieldsRequired() {
        return allFieldsRequired;
    }

    public void setAllFieldsRequired(final boolean allFieldsRequired) {
        this.allFieldsRequired = allFieldsRequired;
    }

    public MapElementGenerator allFieldsRequired() {
        this.allFieldsRequired = true;
        return this;
    }

    public MapElementGenerator allFieldsRequired(final boolean allFieldsRequired) {
        this.allFieldsRequired = allFieldsRequired;
        return this;
    }

    public PropertiesFilter getMapValidator() {
        return mapValidator;
    }

    public void setMapValidator(final PropertiesFilter validator) {
        this.mapValidator = validator;
    }

    public MapElementGenerator mapValidator(final PropertiesFilter mapValidator) {
        requireNonNull(mapValidator, "mapValidator is required");
        this.mapValidator = mapValidator;
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

    public MapElementGenerator skipInvalid() {
        this.skipInvalid = true;
        return this;
    }

    public MapElementGenerator skipInvalid(final boolean skipInvalid) {
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

    public MapElementGenerator transformer(final PropertiesTransformer transformer) {
        requireNonNull(transformer, "transformer is required");
        this.transformer = transformer;
        return this;
    }

    public List<TupleAdaptedFunction<String, ?, ?>> getMapTransforms() {
        return null != transformer ? transformer.getComponents() : null;
    }

    public void setMapTransforms(final List<TupleAdaptedFunction<String, ?, ?>> transformeFunctions) {
        requireNonNull(transformer, "transformer is required");
        this.transformer = new PropertiesTransformer();
        this.transformer.setComponents(transformeFunctions);
    }

    public ElementGenerator<Element> getFollowOnGenerator() {
        return followOnGenerator;
    }

    public void setFollowOnGenerator(final ElementGenerator<Element> followOnGenerator) {
        this.followOnGenerator = followOnGenerator;
    }

    public MapElementGenerator followOnGenerator(final ElementGenerator<Element> followOnGenerator) {
        this.followOnGenerator = followOnGenerator;
        return this;
    }

}

/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element.function;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.commonutil.iterable.StreamIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition.DESTINATION;
import static uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition.DIRECTED;
import static uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition.GROUP;
import static uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition.SOURCE;
import static uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition.VERTEX;

@Since("1.8.0")
@Summary("Generates elements from a tuple")
@JsonPropertyOrder(alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class TupleToElements extends KorypheFunction<Tuple<String>, Iterable<Element>> implements Serializable {
    private static final long serialVersionUID = -6793331642118688901L;

    private final List<ElementTupleDefinition> elements = new ArrayList<>();

    @Override
    public Iterable<Element> apply(final Tuple<String> tuple) {
        return new StreamIterable<>(() -> elements.stream().map(e -> createElement(tuple, e)).filter(new Exists()));
    }

    private Element createElement(final Tuple<String> tuple, final ElementTupleDefinition elementDef) {
        requireNonNull(elementDef.get(GROUP), GROUP + " is required");
        Element element = null;
        if (elementDef.containsKey(VERTEX)) {
            final Object vertex = getField(VERTEX, elementDef, tuple);
            if (nonNull(vertex)) {
                element = new Entity(elementDef.getGroup(), vertex);
            }
        } else {
            final Object source = getField(SOURCE, elementDef, tuple);
            final Object destination = getField(DESTINATION, elementDef, tuple);
            Object directed = getField(DIRECTED, elementDef, tuple);
            directed = isNull(directed) || Boolean.TRUE.equals(directed) || (directed instanceof String && Boolean.parseBoolean((String) directed));
            if (nonNull(source) && nonNull(destination)) {
                element = new Edge(elementDef.getGroup(), source, destination, (boolean) directed);
            }
        }

        if (nonNull(element)) {
            for (final Map.Entry<String, Object> entry : elementDef.entrySet()) {
                final IdentifierType id = IdentifierType.fromName(entry.getKey());
                if (null == id) {
                    element.putProperty(entry.getKey(), getField(entry.getValue(), tuple));
                }
            }
        }

        return element;
    }

    private Object getField(final String key, final ElementTupleDefinition elementDef, final Tuple<String> tuple) {
        return getField(elementDef.get(key), tuple);
    }

    private Object getField(final Object value, final Tuple<String> tuple) {
        if (null == value) {
            return null;
        }

        if (value instanceof String) {
            return tuple.get(((String) value));
        }
        return value;
    }


    public List<ElementTupleDefinition> getElements() {
        return elements;
    }

    public void setElements(final List<ElementTupleDefinition> elements) {
        this.elements.clear();
        this.elements.addAll(elements);
    }

    public TupleToElements element(final ElementTupleDefinition elementDef) {
        elements.add(elementDef);
        return this;
    }

    public TupleToElements elements(final List<ElementTupleDefinition> elementDef) {
        elements.addAll(elementDef);
        return this;
    }
}

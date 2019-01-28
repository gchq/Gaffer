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
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        return new StreamIterable<>(() -> elements.stream().map(e -> createElement(tuple, e)));
    }

    private Element createElement(final Tuple<String> tuple, final ElementTupleDefinition elementDef) {
        requireNonNull(elementDef.get(GROUP), GROUP + " is required");
        final Element element;
        if (elementDef.containsKey(VERTEX)) {
            element = new Entity(elementDef.getGroup(), getField(VERTEX, elementDef, tuple));
        } else {
            element = new Edge(
                    elementDef.getGroup(),
                    getField(SOURCE, elementDef, tuple),
                    getField(DESTINATION, elementDef, tuple),
                    (boolean) getField(DIRECTED, elementDef, tuple)
            );
        }

        for (final Map.Entry<String, Object> entry : elementDef.entrySet()) {
            final IdentifierType id = IdentifierType.fromName(entry.getKey());
            if (null == id) {
                element.putProperty(entry.getKey(), getField(entry.getValue(), tuple));
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
            final Object propValue = tuple.get(((String) value));
            if (null != propValue) {
                return propValue;
            }
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

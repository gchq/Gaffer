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

import uk.gov.gchq.gaffer.commonutil.iterable.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Since("1.8.0")
@Summary("Generates elements from tuples")
@JsonPropertyOrder(alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class TuplesToElements extends KorypheFunction<Iterable<Tuple<String>>, Iterable<Element>> implements Serializable {
    private static final long serialVersionUID = 8954092895764615233L;
    private final List<ElementTupleDefinition> elements = new ArrayList<>();

    @Override
    public Iterable<Element> apply(final Iterable<Tuple<String>> tuples) {
        final TupleToElements tupleToElements = new TupleToElements().elements(elements);
        return new TransformOneToManyIterable<Tuple<String>, Element>(tuples) {
            @Override
            protected Iterable<Element> transform(final Tuple<String> tuple) {
                return tupleToElements.apply(tuple);
            }
        };
    }

    public List<ElementTupleDefinition> getElements() {
        return elements;
    }

    public void setElements(final List<ElementTupleDefinition> elements) {
        this.elements.clear();
        this.elements.addAll(elements);
    }

    public TuplesToElements element(final ElementTupleDefinition elementDef) {
        elements.add(elementDef);
        return this;
    }

    public TuplesToElements elements(final List<ElementTupleDefinition> elementDef) {
        elements.addAll(elementDef);
        return this;
    }
}

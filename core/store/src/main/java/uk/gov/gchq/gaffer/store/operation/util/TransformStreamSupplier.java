/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.util;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.stream.StreamSupplier;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;

/**
 * A <code>TransformStreamSupplier</code> is a {@link StreamSupplier} which uses a
 * {@link Transform} to perform a transformation on each {@link Element} in the input
 * {@link Iterable}, and producing an {@link Iterable}. Only supports {@link Edge} and {@link Entity}
 */
public class TransformStreamSupplier implements StreamSupplier<Element> {
    private final Iterable<? extends Element> input;
    private final Transform transform;

    /**
     * Default constructor.
     *
     * @param operation the Transform to be applied, to the input iterable
     *                  which it contains
     */
    public TransformStreamSupplier(final Transform operation) {
        this.input = operation.getInput();
        this.transform = operation;
    }

    @Override
    public void close() throws IOException {
        CloseableUtil.close(input);
    }

    @Override
    public Stream<Element> get() {
        final Map<String, ElementTransformer> edgesTransform = transform.getEdges();
        final Map<String, ElementTransformer> entitiesTransform = transform.getEntities();

        final Function<Element, Element> toTransformedElement = e -> {
            final String group = e.getGroup();
            final Element rtn;

            if (e instanceof Edge && nonNull(edgesTransform) && nonNull(group)) {
                rtn = edgesTransform.get(group).apply(e);
            } else if (e instanceof Entity && nonNull(entitiesTransform) && nonNull(group)) {
                rtn = entitiesTransform.get(group).apply(e);
            } else if (e instanceof Edge || e instanceof Entity) {
                //either the group or transformer was null for the Edge/Entity.
                rtn = e;
            } else {
                throw new IllegalArgumentException("Transform is only compatible with Edge or Entity elements, found: " + e.getClass());
            }
            return rtn;
        };

        return Streams.toStream((Iterable<Element>) input).map(toTransformedElement);
    }
}

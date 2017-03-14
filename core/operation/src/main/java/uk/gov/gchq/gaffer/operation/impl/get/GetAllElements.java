/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.get;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.IterableOutput;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Map;

/**
 * Extends {@link GetAllElements}, but fetches all elements from the graph that are
 * compatible with the provided view.
 * There are also various flags to filter out the elements returned.
 *
 * @param <E> the element return type
 */
public class GetAllElements<E extends Element> implements
        Operation,
        IterableOutput<E>,
        GraphFilters,
        Options {
    private View view;
    private GraphFilters.DirectedType directedType;
    private Map<String, String> options;

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        this.view = view;
    }

    @Override
    public GraphFilters.DirectedType getDirectedType() {
        return directedType;
    }

    @Override
    public void setDirectedType(final GraphFilters.DirectedType directedType) {
        this.directedType = directedType;
    }

    @Override
    public TypeReference<CloseableIterable<E>> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.CloseableIterableElement();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static class Builder<E extends Element> extends Operation.BaseBuilder<GetAllElements<E>, Builder<E>>
            implements IterableOutput.Builder<GetAllElements<E>, E, Builder<E>>,
            GraphFilters.Builder<GetAllElements<E>, Builder<E>>,
            Options.Builder<GetAllElements<E>, Builder<E>> {
        public Builder() {
            super(new GetAllElements<>());
        }
    }
}

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

package uk.gov.gchq.gaffer.data.element;

import uk.gov.gchq.gaffer.data.element.id.DirectedType;

/**
 * An {@code LazyEdge} wraps an {@link uk.gov.gchq.gaffer.data.element.Edge} and lazily loads identifiers and properties when
 * requested using a provided {@link uk.gov.gchq.gaffer.data.element.ElementValueLoader}. This will avoid loading all of an
 * edge's properties just for it to be filtered out by a filter function.
 */
public class LazyEdge extends Edge {
    private static final long serialVersionUID = 3950963135470686691L;
    private final Edge edge;
    private final ElementValueLoader valueLoader;
    private final LazyProperties lazyProperties;
    private boolean identifiersLoaded = false;

    /**
     * Constructs a  by wrapping the provided {@link uk.gov.gchq.gaffer.data.element.Edge}
     * and using the {@link uk.gov.gchq.gaffer.data.element.ElementValueLoader} to lazily load the element's identifiers and
     * properties when requested.
     *
     * @param edge        the edge to wrap.
     * @param valueLoader the element value loader to use to lazily load the element's identifiers and properties
     */
    public LazyEdge(final Edge edge, final ElementValueLoader valueLoader) {
        this(edge, valueLoader, new LazyProperties(edge.getProperties(), valueLoader));
    }

    protected LazyEdge(final Edge edge, final ElementValueLoader valueLoader, final LazyProperties lazyProperties) {
        super(edge.getGroup(), null, null, false);
        this.edge = edge;
        this.valueLoader = valueLoader;
        this.lazyProperties = lazyProperties;
    }

    @Override
    public void setIdentifiers(final Object source, final Object destination, final boolean directed, final MatchedVertex matchedVertex) {
        edge.setIdentifiers(source, destination, directed, matchedVertex);
        identifiersLoaded = true;
    }

    @Override
    public Object getProperty(final String name) {
        return lazyProperties.get(name);
    }

    @Override
    public Object getSource() {
        loadIdentifiers();
        return edge.getSource();
    }

    @Override
    public Object getDestination() {
        loadIdentifiers();
        return edge.getDestination();
    }

    @Override
    public boolean isDirected() {
        loadIdentifiers();
        return edge.isDirected();
    }

    @Override
    public void putIdentifier(final IdentifierType name, final Object value) {
        edge.putIdentifier(name, value);
    }

    @Override
    public void setIdentifiers(final Object source, final Object destination, final DirectedType directedType) {
        edge.setIdentifiers(source, destination, directedType);
        identifiersLoaded = true;
    }

    @Override
    public void putProperty(final String name, final Object value) {
        lazyProperties.put(name, value);
    }

    @Override
    public String getGroup() {
        return edge.getGroup();
    }

    @Override
    public Edge getElement() {
        return edge;
    }

    @Override
    public LazyProperties getProperties() {
        return lazyProperties;
    }

    @Override
    public boolean equals(final Object element) {
        return edge.equals(element);
    }

    @Override
    public int hashCode() {
        return edge.hashCode();
    }

    private void loadIdentifiers() {
        if (!identifiersLoaded) {
            valueLoader.loadIdentifiers(edge);
            identifiersLoaded = true;
        }
    }
}


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

import java.util.HashSet;
import java.util.Set;

/**
 * An <code>LazyEdge</code> wraps an {@link uk.gov.gchq.gaffer.data.element.Edge} and lazily loads identifiers and properties when
 * requested using a provided {@link uk.gov.gchq.gaffer.data.element.ElementValueLoader}. This will avoid loading all of an
 * edge's properties just for it to be filtered out by a filter function.
 */
public class LazyEdge extends Edge {
    private static final long serialVersionUID = 3950963135470686691L;
    private final Edge edge;
    private final ElementValueLoader valueLoader;
    private final Set<IdentifierType> loadedIdentifiers;
    private final LazyProperties lazyProperties;

    /**
     * Constructs a {@link uk.gov.gchq.gaffer.data.element.LazyEdge} by wrapping the provided {@link uk.gov.gchq.gaffer.data.element.Edge}
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
        super(edge.getGroup());
        this.edge = edge;
        this.valueLoader = valueLoader;
        this.lazyProperties = lazyProperties;
        loadedIdentifiers = new HashSet<>();
    }

    @Override
    public Object getProperty(final String name) {
        return lazyProperties.get(name);
    }

    @Override
    public Object getSource() {
        return lazyLoadIdentifier(edge.getSource(), IdentifierType.SOURCE);
    }

    @Override
    public Object getDestination() {
        return lazyLoadIdentifier(edge.getDestination(), IdentifierType.DESTINATION);
    }

    @Override
    public boolean isDirected() {
        if (!loadedIdentifiers.contains(IdentifierType.DIRECTED)) {
            return edge.isDirected();
        }

        return (boolean) lazyLoadIdentifier(IdentifierType.DIRECTED);
    }

    @Override
    public void setSource(final Object source) {
        edge.setSource(source);
        loadedIdentifiers.add(IdentifierType.SOURCE);
    }

    @Override
    public void setDestination(final Object destination) {
        edge.setDestination(destination);
        loadedIdentifiers.add(IdentifierType.DESTINATION);
    }

    @Override
    public void setDirected(final boolean directed) {
        edge.setDirected(directed);
        loadedIdentifiers.add(IdentifierType.DIRECTED);
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

    private Object lazyLoadIdentifier(final Object currentValue, final IdentifierType name) {
        Object value = currentValue;
        if (null == value && !loadedIdentifiers.contains(name)) {
            value = lazyLoadIdentifier(name);
        }

        return value;
    }

    private Object lazyLoadIdentifier(final IdentifierType name) {
        Object value = valueLoader.getIdentifier(name);
        putIdentifier(name, value);

        return value;
    }
}


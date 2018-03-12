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

package uk.gov.gchq.gaffer.data.element;

/**
 * An {@code LazyEntity} wraps an {@link uk.gov.gchq.gaffer.data.element.Entity} and lazily loads the identifier and properties when
 * requested using a provided {@link uk.gov.gchq.gaffer.data.element.ElementValueLoader}. This will avoid loading all of an
 * entity's properties just for it to be filtered out by a filter function.
 */
public class LazyEntity extends Entity {
    private static final long serialVersionUID = 8067424362415322354L;
    private final Entity entity;
    private final ElementValueLoader valueLoader;
    private final LazyProperties lazyProperties;
    private boolean identifiersLoaded = false;

    /**
     * Constructs a  by wrapping the provided {@link uk.gov.gchq.gaffer.data.element.Entity}
     * and using the {@link uk.gov.gchq.gaffer.data.element.ElementValueLoader} to lazily load the element's identifiers and
     * properties when requested.
     *
     * @param entity      the entity to wrap.
     * @param valueLoader the element value loader to use to lazily load the element's identifiers and properties
     */
    public LazyEntity(final Entity entity, final ElementValueLoader valueLoader) {
        this(entity, valueLoader, new LazyProperties(entity.getProperties(), valueLoader));
    }

    protected LazyEntity(final Entity entity, final ElementValueLoader valueLoader, final LazyProperties lazyProperties) {
        super(entity.getGroup(), null);
        this.entity = entity;
        this.valueLoader = valueLoader;
        this.lazyProperties = lazyProperties;
    }

    @Override
    public Object getProperty(final String name) {
        return lazyProperties.get(name);
    }

    @Override
    public Object getVertex() {
        loadIdentifiers();
        return entity.getVertex();
    }

    @Override
    public void setVertex(final Object vertex) {
        entity.setVertex(vertex);
        identifiersLoaded = true;
    }

    @Override
    public void putProperty(final String name, final Object value) {
        lazyProperties.put(name, value);
    }

    @Override
    public String getGroup() {
        return entity.getGroup();
    }

    @Override
    public Entity getElement() {
        return entity;
    }

    @Override
    public LazyProperties getProperties() {
        return lazyProperties;
    }

    @Override
    public boolean equals(final Object element) {
        return entity.equals(element);
    }

    @Override
    public void putIdentifier(final IdentifierType name, final Object value) {
        entity.putIdentifier(name, value);
        identifiersLoaded = true;
    }

    @Override
    public int hashCode() {
        return entity.hashCode();
    }

    private void loadIdentifiers() {
        if (!identifiersLoaded) {
            valueLoader.loadIdentifiers(entity);
            identifiersLoaded = true;
        }
    }
}


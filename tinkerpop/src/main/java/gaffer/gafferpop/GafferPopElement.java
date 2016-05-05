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
package gaffer.gafferpop;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * A <code>GafferPopElement</code> is an {@link Element}.
 * The remove method is not supported.
 * As Gaffer does not allow updates, there is a readOnly flag that can be set
 * to prevent changes to the element.
 */
public abstract class GafferPopElement implements Element {
    protected final Object id;
    protected final String label;
    private boolean readOnly = false;
    private final GafferPopGraph graph;

    protected GafferPopElement(final String label, final Object id, final GafferPopGraph graph) {
        this.id = id;
        this.label = label;
        this.graph = graph;
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    public void setReadOnly() {
        this.readOnly = true;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Elements cannot be removed");
    }

    @Override
    public GafferPopGraph graph() {
        return graph;
    }
}

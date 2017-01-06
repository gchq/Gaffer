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

package gaffer.data.element;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang.builder.EqualsBuilder;

/**
 * An <code>Edge</code> in an {@link gaffer.data.element.Element} containing a source, destination and a directed flag.
 * The source and destination vertices can be any type of {@link java.lang.Object}.
 * There is no requirement for these vertices to connect to an {@link gaffer.data.element.Entity} vertex -
 * for example you could have a 'graph' of just edges.
 * Edges are designed so that multiple edges can share the same identifiers but are distinguished via their
 * group.
 *
 * @see gaffer.data.element.Edge.Builder
 */
public class Edge extends Element<EdgeId> {
    private static final long serialVersionUID = -1263742403728471638L;

    Edge() {
        super(new EdgeId());
    }

    public Edge(final String group) {
        super(group, new EdgeId());
    }

    public Edge(final String group, final Object source, final Object destination, final boolean directed) {
        super(group, new EdgeId(source, destination, directed));
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Object getSource() {
        return getId().getSource();
    }

    public void setSource(final Object source) {
        getId().setSource(source);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Object getDestination() {
        return getId().getDestination();
    }

    public void setDestination(final Object destination) {
        getId().setDestination(destination);
    }

    public boolean isDirected() {
        return getId().isDirected();
    }

    public void setDirected(final boolean directed) {
        getId().setDirected(directed);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof Edge)
                && equals((Edge) obj);
    }

    public boolean equals(final Edge edge) {
        return null != edge
                && new EqualsBuilder()
                .appendSuper(super.equals(edge))
                .isEquals();
    }

    @Override
    public Edge emptyClone() {
        return new Edge(
                this.getGroup(),
                this.getSource(),
                this.getDestination(),
                this.isDirected()
        );
    }

    @Override
    public String toString() {
        return "Edge{"
                + getId()
                + super.toString()
                + "} ";
    }

    public static class Builder {
        private final Edge edge = new Edge();

        public Builder group(final String group) {
            edge.setGroup(group);
            return this;
        }

        public Builder source(final Object source) {
            edge.setSource(source);
            return this;
        }

        public Builder dest(final Object dest) {
            edge.setDestination(dest);
            return this;
        }

        public Builder directed(final boolean directed) {
            edge.setDirected(directed);
            return this;
        }

        public Builder property(final String name, final Object value) {
            edge.putProperty(name, value);
            return this;
        }

        public Edge build() {
            return edge;
        }
    }
}


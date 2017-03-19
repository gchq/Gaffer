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


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An <code>Edge</code> in an {@link uk.gov.gchq.gaffer.data.element.Element} containing a source, destination and a directed flag.
 * The source and destination vertices can be any type of {@link java.lang.Object}.
 * There is no requirement for these vertices to connect to an {@link uk.gov.gchq.gaffer.data.element.Entity} vertex -
 * for example you could have a 'graph' of just edges.
 * Edges are designed so that multiple edges can share the same identifiers but are distinguished via their
 * group.
 *
 * @see uk.gov.gchq.gaffer.data.element.Edge.Builder
 */
public class Edge extends Element {
    private static final Logger LOGGER = LoggerFactory.getLogger(Edge.class);
    private static final long serialVersionUID = -5596452468277807842L;
    private Object source;
    private Object destination;
    private boolean directed;

    Edge() {
        super();
    }

    public Edge(final String group) {
        super(group);
    }

    public Edge(final String group, final Object source, final Object destination, final boolean directed) {
        super(group);
        this.source = source;
        this.destination = destination;
        this.directed = directed;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Object getSource() {
        return source;
    }

    public void setSource(final Object source) {
        this.source = source;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Object getDestination() {
        return destination;
    }

    public void setDestination(final Object destination) {
        this.destination = destination;
    }

    public boolean isDirected() {
        return directed;
    }

    public void setDirected(final boolean directed) {
        this.directed = directed;
    }

    @Override
    public Object getIdentifier(final IdentifierType identifierType) {
        switch (identifierType) {
            case SOURCE:
                return getSource();
            case DESTINATION:
                return getDestination();
            case DIRECTED:
                return isDirected();
            default:
                return null;
        }
    }

    @Override
    public void putIdentifier(final IdentifierType identifierType, final Object propertyToBeSet) {
        switch (identifierType) {
            case SOURCE:
                setSource(propertyToBeSet);
                break;
            case DESTINATION:
                setDestination(propertyToBeSet);
                break;
            case DIRECTED:
                setDirected((boolean) propertyToBeSet);
                break;
            default:
                LOGGER.error("Unknown identifier type: " + identifierType + " detected.");
        }
    }

    @Override
    public int hashCode() {
        int hash;
        if (directed) {
            hash = new HashCodeBuilder(21, 3)
                    .appendSuper(super.hashCode())
                    .append(source)
                    .append(destination)
                    .append(directed)
                    .toHashCode();
        } else {
            hash = super.hashCode();
            hash ^= source.hashCode();
            hash ^= destination.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof Edge)
                && equals((Edge) obj);
    }

    public boolean equals(final Edge edge) {
        return null != edge
                && (new EqualsBuilder()
                .appendSuper(super.equals(edge))
                .append(source, edge.getSource())
                .append(destination, edge.getDestination())
                .append(directed, edge.isDirected())
                .isEquals()
                || new EqualsBuilder()
                .appendSuper(super.equals(edge))
                .append(source, edge.getDestination())
                .append(destination, edge.getSource())
                .append(directed, false)
                .isEquals()
        );
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
                + "source=" + source
                + ", destination=" + destination
                + ", directed=" + directed
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


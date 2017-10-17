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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.comparison.ComparableOrToStringComparator;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;

import java.util.Comparator;

/**
 * An {@code Edge} in an {@link uk.gov.gchq.gaffer.data.element.Element} containing a source, destination and a directed flag.
 * The source and destination vertices can be any type of {@link java.lang.Object}.
 * There is no requirement for these vertices to connect to an {@link uk.gov.gchq.gaffer.data.element.Entity} vertex -
 * for example you could have a 'graph' of just edges.
 * Edges are designed so that multiple edges can share the same identifiers but are distinguished via their
 * group.
 *
 * @see uk.gov.gchq.gaffer.data.element.Edge.Builder
 */
public class Edge extends Element implements EdgeId {
    private static final Logger LOGGER = LoggerFactory.getLogger(Edge.class);
    private static final long serialVersionUID = -5596452468277807842L;
    private static final Comparator<Object> VERTEX_COMPARATOR = new ComparableOrToStringComparator();
    private Object source;
    private Object destination;
    private boolean directed;
    private MatchedVertex matchedVertex;

    Edge() {
        super();
    }

    public Edge(final String group) {
        super(group);
    }

    /**
     * Constructs an instance of Edge.
     * <p>
     * If the edge is undirected the the source and destination vertices may get
     * swapped to ensure undirected edges are consistently constructed.
     * </p>
     *
     * @param group       the Edge group
     * @param source      the source vertex
     * @param destination the destination vertex
     * @param directed    true if the edge is directed
     */
    public Edge(final String group, final Object source, final Object destination, final boolean directed) {
        this(group, source, destination, directed, null, null);
    }

    /**
     * Constructs an instance of Edge.
     * <p>
     * If the edge is undirected the the source and destination vertices may get
     * swapped to ensure undirected edges are consistently constructed.
     * </p>
     *
     * @param group         the Edge group
     * @param source        the source vertex
     * @param destination   the destination vertex
     * @param directed      true if the edge is directed
     * @param matchedVertex used at query time to mark which vertex was matched.
     * @param properties    the edge properties
     */
    public Edge(final String group,
                final Object source,
                final Object destination,
                final boolean directed,
                final MatchedVertex matchedVertex,
                final Properties properties) {
        super(group, properties);
        this.source = source;
        this.destination = destination;
        this.directed = directed;
        this.matchedVertex = matchedVertex;
        orderVertices();
    }

    /**
     * Constructs an instance of Edge.
     * <p>
     * If the edge is undirected the the source and destination vertices may get
     * swapped to ensure undirected edges are consistently constructed.
     * </p>
     *
     * @param group         the Edge group
     * @param source        the source vertex
     * @param destination   the destination vertex
     * @param directed      true if the edge is directed
     * @param directedType  the direction of the edge
     * @param matchedVertex used at query time to mark which vertex was matched.
     * @param properties    the edge properties
     */
    @JsonCreator
    Edge(@JsonProperty("group") final String group,
         @JsonProperty("source") final Object source,
         @JsonProperty("destination") final Object destination,
         @JsonProperty("directed") final Boolean directed,
         @JsonProperty("directedType") final DirectedType directedType,
         @JsonProperty("matchedVertex") final MatchedVertex matchedVertex,
         @JsonProperty("properties") final Properties properties) {
        this(group, source, destination, getDirected(directed, directedType), matchedVertex, properties);
    }

    @Override
    public Object getSource() {
        return source;
    }

    @Override
    public Object getDestination() {
        return destination;
    }

    @JsonIgnore
    @Override
    public DirectedType getDirectedType() {
        if (isDirected()) {
            return DirectedType.DIRECTED;
        }

        return DirectedType.UNDIRECTED;
    }

    @JsonIgnore(false)
    @JsonGetter("directed")
    @Override
    public boolean isDirected() {
        return directed;
    }

    @Override
    public MatchedVertex getMatchedVertex() {
        return matchedVertex;
    }

    @JsonIgnore
    public Object getMatchedVertexValue() {
        return MatchedVertex.DESTINATION == matchedVertex ? getDestination() : getSource();
    }

    @JsonIgnore
    public Object getAdjacentMatchedVertexValue() {
        return MatchedVertex.DESTINATION == matchedVertex ? getSource() : getDestination();
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
            case MATCHED_VERTEX:
                return getMatchedVertexValue();
            case ADJACENT_MATCHED_VERTEX:
                return getAdjacentMatchedVertexValue();
            default:
                return null;
        }
    }

    /**
     * This may cause undirected edges to be inconsistent as the source and
     * destination values may not be ordered correctly.
     *
     * @param identifierType the identifier type to update
     * @param value          the identifier value
     */
    @Override
    void putIdentifier(final IdentifierType identifierType, final Object value) {
        switch (identifierType) {
            case SOURCE:
            case MATCHED_VERTEX:
                source = value;
                break;
            case DESTINATION:
            case ADJACENT_MATCHED_VERTEX:
                destination = value;
                break;
            case DIRECTED:
                directed = (null != value && (value instanceof Boolean && (boolean) value) || (value instanceof DirectedType && ((DirectedType) value).isDirected()));
                break;
            default:
                LOGGER.error("Unknown identifier type: {} detected.", identifierType);
        }
    }

    /**
     * Sets the identifiers for an Edge.
     * <p>
     * If the edge is undirected the the source and destination vertices may get
     * swapped to ensure undirected edges are consistently constructed.
     * </p>
     *
     * @param source       the source vertex
     * @param destination  the destination vertex
     * @param directedType the edge directedType
     */
    @Override
    public void setIdentifiers(final Object source, final Object destination, final DirectedType directedType) {
        setIdentifiers(source, destination, directedType.isDirected());
    }

    /**
     * Sets the identifiers for an Edge.
     * <p>
     * If the edge is undirected the the source and destination vertices may get
     * swapped to ensure undirected edges are consistently constructed.
     * </p>
     *
     * @param source      the source vertex
     * @param destination the destination vertex
     * @param directed    true if the edge is directed
     */
    public void setIdentifiers(final Object source, final Object destination, final boolean directed) {
        setIdentifiers(source, destination, directed, matchedVertex);
    }

    /**
     * Sets the identifiers for an Edge.
     * <p>
     * If the edge is undirected the the source and destination vertices may get
     * swapped to ensure undirected edges are consistently constructed.
     * </p>
     *
     * @param source        the source vertex
     * @param destination   the destination vertex
     * @param directedType  the edge directedType
     * @param matchedVertex the vertex that matched a query seed
     */
    @Override
    public void setIdentifiers(final Object source, final Object destination, final DirectedType directedType, final MatchedVertex matchedVertex) {
        setIdentifiers(source, destination, directedType.isDirected(), matchedVertex);
    }

    /**
     * Sets the identifiers for an Edge.
     * <p>
     * If the edge is undirected the the source and destination vertices may get
     * swapped to ensure undirected edges are consistently constructed.
     * </p>
     *
     * @param source        the source vertex
     * @param destination   the destination vertex
     * @param directed      true if the edge is directed
     * @param matchedVertex the vertex that matched a query seed
     */
    public void setIdentifiers(final Object source, final Object destination, final boolean directed, final MatchedVertex matchedVertex) {
        this.source = source;
        this.destination = destination;
        this.directed = directed;
        this.matchedVertex = matchedVertex;
        orderVertices();
    }

    private void orderVertices() {
        if (!directed) {
            if (VERTEX_COMPARATOR.compare(source, destination) > 0) {
                swapVertices();
            }
        }
    }

    private void swapVertices() {
        final Object tmp = this.source;
        this.source = this.destination;
        this.destination = tmp;
        if (matchedVertex != null) {
            if (matchedVertex == MatchedVertex.DESTINATION) {
                this.matchedVertex = MatchedVertex.SOURCE;
            } else {
                this.matchedVertex = MatchedVertex.DESTINATION;
            }
        }
    }

    /**
     * Note this does not include the matchedVertex field.
     *
     * @return a hash code value for this edge.
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(21, 3)
                .appendSuper(super.hashCode())
                .append(getSource())
                .append(getDestination())
                .append(isDirected())
                .toHashCode();
    }

    /**
     * Note this does not include the matchedVertex field.
     *
     * @param obj the reference object with which to compare.
     * @return {@code true} if this object is the same as the obj
     * argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof Edge)
                && equals((Edge) obj);
    }

    /**
     * Note this does not include the matchedVertex field.
     *
     * @param edge the reference Edge with which to compare.
     * @return {@code true} if this object is the same as the edge
     * argument; {@code false} otherwise.
     */
    public boolean equals(final Edge edge) {
        return null != edge
                && new EqualsBuilder()
                .append(isDirected(), edge.isDirected())
                .append(getSource(), edge.getSource())
                .append(getDestination(), edge.getDestination())
                .appendSuper(super.equals(edge))
                .isEquals();
    }

    @Override
    public Edge emptyClone() {
        return new Edge(
                getGroup(),
                getSource(),
                getDestination(),
                isDirected(),
                getMatchedVertex(),
                new Properties()
        );
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("source", getSource())
                .append("destination", getDestination())
                .append("directed", isDirected())
                .append("matchedVertex", getMatchedVertex())
                .appendSuper(super.toString())
                .build();
    }

    private static boolean getDirected(final Boolean directed, final DirectedType directedType) {
        if (null != directed) {
            if (null != directedType) {
                throw new IllegalArgumentException("Use either 'directed' or 'directedType' - not both.");
            }
            return directed;
        }
        return DirectedType.isDirected(directedType);
    }

    public static class Builder {
        private String group = Element.DEFAULT_GROUP;
        private Object source;
        private Object dest;
        private boolean directed;
        private MatchedVertex matchedVertex;
        private Properties properties = new Properties();

        public Builder group(final String group) {
            this.group = group;
            return this;
        }

        public Builder source(final Object source) {
            this.source = source;
            return this;
        }

        public Builder dest(final Object dest) {
            this.dest = dest;
            return this;
        }

        public Builder directed(final boolean directed) {
            this.directed = directed;
            return this;
        }

        public Builder matchedVertex(final MatchedVertex matchedVertex) {
            this.matchedVertex = matchedVertex;
            return this;
        }

        public Builder property(final String name, final Object value) {
            properties.put(name, value);
            return this;
        }

        public Edge build() {
            return new Edge(group, source, dest, directed, matchedVertex, properties);
        }
    }
}


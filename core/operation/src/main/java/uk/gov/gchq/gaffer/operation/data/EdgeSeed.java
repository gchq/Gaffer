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

package uk.gov.gchq.gaffer.operation.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.comparison.ComparableOrToStringComparator;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;

import java.util.Comparator;

/**
 * An {@code EdgeSeed} contains source, destination and directed identifiers to identify an
 * {@link uk.gov.gchq.gaffer.data.element.Edge}.
 * It is used as a mainly used as a seed for queries.
 */
public class EdgeSeed extends ElementSeed implements EdgeId {
    private static final long serialVersionUID = -8137886975649690000L;
    private static final Comparator<Object> VERTEX_COMPARATOR = new ComparableOrToStringComparator();
    private Object source;
    private Object destination;
    private DirectedType directed;
    private MatchedVertex matchedVertex;

    public EdgeSeed() {
        this(null, null);
    }

    public EdgeSeed(final Object source, final Object destination) {
        this(source, destination, DirectedType.EITHER);
    }

    public EdgeSeed(final Object source, final Object destination, final boolean directed) {
        this(source, destination, directed ? DirectedType.DIRECTED : DirectedType.UNDIRECTED);
    }

    public EdgeSeed(final Object source, final Object destination, final DirectedType directed) {
        this(source, destination, directed, MatchedVertex.SOURCE);
    }

    public EdgeSeed(final Object source, final Object destination, final boolean directed, final MatchedVertex matchedVertex) {
        this(source, destination, directed ? DirectedType.DIRECTED : DirectedType.UNDIRECTED, matchedVertex);
    }

    public EdgeSeed(final Object source,
                    final Object destination,
                    final DirectedType directed,
                    final MatchedVertex matchedVertex) {
        this.matchedVertex = matchedVertex;
        this.source = source;
        this.destination = destination;
        this.directed = directed;
        this.matchedVertex = matchedVertex;
        orderVertices();
    }

    @JsonCreator
    public EdgeSeed(@JsonProperty("source")  final Object source,
                    @JsonProperty("destination") final Object destination,
                    @JsonProperty("directed") final Boolean directed,
                    @JsonProperty("directedType") final DirectedType directedType,
                    @JsonProperty("matchedVertex") final MatchedVertex matchedVertex) {
        this(source, destination, getDirectedType(directed, directedType), matchedVertex);
    }

    @Override
    public Object getSource() {
        return source;
    }

    @Override
    public Object getDestination() {
        return destination;
    }

    @Override
    public DirectedType getDirectedType() {
        return directed;
    }

    @Override
    public void setIdentifiers(final Object source, final Object destination, final DirectedType directed, final MatchedVertex matchedVertex) {
        this.source = source;
        this.destination = destination;
        this.directed = directed;
        this.matchedVertex = matchedVertex;
        orderVertices();
    }

    @Override
    public MatchedVertex getMatchedVertex() {
        return matchedVertex;
    }

    private void orderVertices() {
        if (!DirectedType.isDirected(directed)) {
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
                matchedVertex = MatchedVertex.SOURCE;
            } else {
                matchedVertex = MatchedVertex.DESTINATION;
            }
        }
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
                && (obj instanceof EdgeSeed)
                && isEqual((EdgeSeed) obj);
    }

    /**
     * Note this does not include the matchedVertex field.
     *
     * @return a hash code value for this edge.
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(21, 73)
                .append(source)
                .append(destination)
                .append(directed)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("source", source)
                .append("destination", destination)
                .append("directed", directed)
                .toString();
    }

    private static DirectedType getDirectedType(final Boolean directed, final DirectedType directedType) {
        if (null != directed) {
            if (null != directedType) {
                throw new IllegalArgumentException("Use either 'directed' or 'directedType' - not both.");
            }
            return directed ? DirectedType.DIRECTED : DirectedType.UNDIRECTED;
        }
        return directedType;
    }
}

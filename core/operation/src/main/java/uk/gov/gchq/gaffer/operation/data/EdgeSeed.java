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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;

/**
 * An <code>EdgeSeed</code> contains source, destination and directed identifiers to identify an
 * {@link uk.gov.gchq.gaffer.data.element.Edge}.
 * It is used as a mainly used as a seed for queries.
 */
public class EdgeSeed extends ElementSeed implements EdgeId {
    private static final long serialVersionUID = -8137886975649690000L;
    private Object source;
    private Object destination;
    private boolean directed;

    public EdgeSeed() {
    }

    public EdgeSeed(final Object source, final Object destination, final boolean directed) {
        this.source = source;
        this.destination = destination;
        this.directed = directed;
    }

    @Override
    public Object getSource() {
        return source;
    }

    @Override
    public void setSource(final Object source) {
        this.source = source;
    }

    @Override
    public Object getDestination() {
        return destination;
    }

    @Override
    public void setDestination(final Object destination) {
        this.destination = destination;
    }

    @Override
    public boolean isDirected() {
        return directed;
    }

    @Override
    public void setDirected(final boolean directed) {
        this.directed = directed;
    }

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof EdgeSeed)
                && equals((EdgeSeed) obj);
    }

    private boolean equals(final EdgeSeed that) {
        return null != that
                && (new EqualsBuilder()
                .append(directed, that.isDirected())
                .append(source, that.getSource())
                .append(destination, that.getDestination())
                .isEquals()
                || new EqualsBuilder()
                .append(directed, false)
                .append(source, that.getDestination())
                .append(destination, that.getSource())
                .isEquals()
        );
    }

    @Override
    public int hashCode() {
        int hash;
        if (directed) {
            hash = new HashCodeBuilder(21, 3)
                    .append(source)
                    .append(destination)
                    .append(directed)
                    .toHashCode();
        } else {
            hash = new HashCodeBuilder(21, 3)
                    .append(directed)
                    .toHashCode();
            hash ^= source.hashCode();
            hash ^= destination.hashCode();
        }
        return hash;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("source", source)
                .append("destination", destination)
                .append("directed", directed)
                .build();
    }
}

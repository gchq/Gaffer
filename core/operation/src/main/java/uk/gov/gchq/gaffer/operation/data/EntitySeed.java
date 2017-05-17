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

import org.apache.commons.lang3.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.id.EntityId;

/**
 * An <code>EntitySeed</code> contains a single vertex for an {@link uk.gov.gchq.gaffer.data.element.Entity}.
 * It is used as a mainly used as a seed for queries.
 */
public class EntitySeed extends ElementSeed implements EntityId {
    private static final long serialVersionUID = -1668220155074029644L;
    private Object vertex;

    public EntitySeed() {
    }

    public EntitySeed(final Object vertex) {
        this.vertex = vertex;
    }

    @Override
    public Object getVertex() {
        return vertex;
    }

    @Override
    public void setVertex(final Object vertex) {
        this.vertex = vertex;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EntitySeed)) {
            return false;
        }

        final EntitySeed that = (EntitySeed) o;
        return !(vertex != null ? !vertex.equals(that.getVertex()) : that.getVertex() != null);
    }

    @Override
    public int hashCode() {
        return vertex != null ? vertex.hashCode() : 0;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("vertex" + vertex)
                .build();
    }
}

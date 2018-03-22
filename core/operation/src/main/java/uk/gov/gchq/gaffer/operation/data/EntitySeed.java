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

package uk.gov.gchq.gaffer.operation.data;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.id.EntityId;

import java.util.Objects;

/**
 * An {@code EntitySeed} contains a single vertex for an {@link uk.gov.gchq.gaffer.data.element.Entity}.
 * It is mainly used as a seed for queries.
 */
@JsonPropertyOrder(value = {"class", "vertex"}, alphabetic = true)
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
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final EntitySeed that = (EntitySeed) obj;

        return new EqualsBuilder()
                .append(vertex, that.vertex)
                .isEquals();
    }

    /*
    Important not to alter hashcode implementation - adopting the ACL3 style
    does not work for a large selection of prime numbers as arguments.
    */
    @Override
    public int hashCode() {
        return Objects.hashCode(vertex);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("vertex", vertex)
                .toString();
    }
}

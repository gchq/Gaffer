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

package uk.gov.gchq.koryphe.composite;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A <code>CompositeFunction</code> is a {@link List} of {@link Function}s that combine to make a composite
 * function.
 *
 * @param <F> The type of Function
 */
public abstract class Composite<F> {
    private List<F> functions;

    public Composite() {
        this(new ArrayList<>());
    }

    public Composite(List<F> functions) {
        this.functions = functions;
    }

    public List<F> getFunctions() {
        return functions;
    }

    public void setFunctions(final List<F> functions) {
        this.functions = functions;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Composite composite = (Composite) o;

        return new EqualsBuilder()
                .append(functions, composite.functions)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getClass())
                .append(functions)
                .toHashCode();
    }
}

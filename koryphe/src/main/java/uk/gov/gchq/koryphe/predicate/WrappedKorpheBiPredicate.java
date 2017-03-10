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
package uk.gov.gchq.koryphe.predicate;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.util.function.BiPredicate;

public class WrappedKorpheBiPredicate<T, U> extends KorpheBiPredicate<T, U> {
    private BiPredicate<T, U> predicate;

    public WrappedKorpheBiPredicate() {
    }

    public WrappedKorpheBiPredicate(final BiPredicate<T, U> prediate) {
        this.predicate = prediate;
    }

    @Override
    public boolean test(final T t, final U u) {
        return null == predicate || predicate.test(t, u);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (!super.equals(other)) {
            return false;
        }

        final WrappedKorpheBiPredicate otherPredicate = (WrappedKorpheBiPredicate) other;
        return new EqualsBuilder()
                .append(predicate, otherPredicate.predicate)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(predicate)
                .toHashCode();
    }

    public BiPredicate<T, U> getPredicate() {
        return predicate;
    }

    public void setPredicate(final BiPredicate<T, U> predicate) {
        this.predicate = predicate;
    }
}

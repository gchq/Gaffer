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
package uk.gov.gchq.koryphe.binaryoperator;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.util.function.BinaryOperator;

public class WrappedKorpheBinaryOperator<T> extends KorpheBinaryOperator<T> {
    private BinaryOperator<T> binaryOperator;

    public WrappedKorpheBinaryOperator() {
    }

    public WrappedKorpheBinaryOperator(final BinaryOperator<T> binaryOperator) {
        this.binaryOperator = binaryOperator;
    }

    @Override
    public T apply(final T t1, final T t2) {
        if (null == binaryOperator) {
            return null;
        }

        return binaryOperator.apply(t1, t2);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }

        if (!super.equals(other)) {
            return false;
        }

        final WrappedKorpheBinaryOperator otherBinaryOperator = (WrappedKorpheBinaryOperator) other;
        return new EqualsBuilder()
                .append(binaryOperator, otherBinaryOperator.binaryOperator)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(binaryOperator)
                .toHashCode();
    }

    public BinaryOperator<T> getBinaryOperator() {
        return binaryOperator;
    }

    public void setBinaryOperator(final BinaryOperator<T> binaryOperator) {
        this.binaryOperator = binaryOperator;
    }
}

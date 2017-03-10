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
package uk.gov.gchq.gaffer.function.transform;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.koryphe.function.KorypheFunction2;

/**
 * An <code>Concat</code> is a {@link java.util.function.Function} that takes in
 * two objects and calls toString on them and concatenates them together. The default separator is a comma,
 * you can set a custom separator using setSeparator(String).
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public class Concat extends KorypheFunction2<Object, Object, String> {
    private static final String DEFAULT_SEPARATOR = ",";
    private String separator = DEFAULT_SEPARATOR;

    @Override
    public String apply(final Object input1, final Object input2) {
        if (null == input1) {
            if (null == input2) {
                return null;
            }
            return String.valueOf(input2);
        }

        if (null == input2) {
            return String.valueOf(input1);
        }

        return input1 + separator + input2;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(final String separator) {
        this.separator = separator;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Concat concat = (Concat) o;

        return new EqualsBuilder()
                .append(separator, concat.separator)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(separator)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("separator", separator)
                .toString();
    }
}

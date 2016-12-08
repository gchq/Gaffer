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
package uk.gov.gchq.gaffer.function.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleFilterFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

@Inputs(String.class)
public class MultiRegex extends SimpleFilterFunction<String> {
    private Pattern[] patterns;

    public MultiRegex() {
        this(null);
    }

    public MultiRegex(final Pattern[] patterns) {
        setPatterns(patterns);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonProperty("value")
    public Pattern[] getPatterns() {
        return Arrays.copyOf(patterns, patterns.length);
    }

    public void setPatterns(final Pattern[] patterns) {
        if (null != patterns) {
            this.patterns = Arrays.copyOf(patterns, patterns.length);
        } else {
            this.patterns = new Pattern[0];
        }
    }

    @Override
    public boolean isValid(final String input) {
        if (null == input || input.getClass() != String.class) {
            return false;
        }
        for (final Pattern pattern : patterns) {
            if (pattern.matcher(input).matches()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public MultiRegex statelessClone() {
        return new MultiRegex(patterns);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MultiRegex that = (MultiRegex) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(patternsToStrings(patterns), patternsToStrings(that.patterns))
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(patternsToStrings(patterns))
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("patterns", patterns)
                .toString();
    }

    /**
     * Utility method to convert an array of {@link Pattern}s to
     * and array of {@link String}s.
     *
     * This is required since the Pattern class does not override {@link Object#equals(Object)}
     * or {@link Object#hashCode()}
     *
     * @param patterns an array of Patterns to convert
     * @return an array of Strings representing the regex pattern
     */
    private String[] patternsToStrings(final Pattern[] patterns) {
        final List<String> strings = new ArrayList<>(patterns.length);

        for (final Pattern pattern : patterns) {
            strings.add(pattern.toString());
        }

        return strings.toArray(new String[]{});
    }
}

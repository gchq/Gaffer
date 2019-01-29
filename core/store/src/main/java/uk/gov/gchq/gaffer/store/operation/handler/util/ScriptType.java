/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.util;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static java.util.Objects.nonNull;

/**
 * The type of a script, defined by a regex, along a handler for executing the script.
 */
public class ScriptType {
    private Pattern regex;
    private BiFunction<Object, String, Object> handler;

    public ScriptType() {
    }

    public ScriptType(final Pattern regex, final BiFunction<Object, String, Object> handler) {
        this.regex = regex;
        this.handler = handler;
    }

    public ScriptType(final String regex, final BiFunction<Object, String, Object> handler) {
        setRegex(regex);
        this.handler = handler;
    }

    public Pattern getRegex() {
        return regex;
    }

    @JsonGetter("regex")
    public String getRegexString() {
        return nonNull(regex) ? regex.pattern() : null;
    }

    public void setRegex(final String regex) {
        this.regex = Pattern.compile(regex);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    public BiFunction<Object, String, Object> getHandler() {
        return handler;
    }

    public void setHandler(final BiFunction<Object, String, Object> handler) {
        this.handler = handler;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScriptType that = (ScriptType) o;

        return new EqualsBuilder()
                .append(getRegexString(), that.getRegexString())
                .append(handler, that.handler)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 31)
                .append(getRegexString())
                .append(handler)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("regex", getRegexString())
                .append("handler", handler)
                .toString();
    }
}

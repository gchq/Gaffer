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
package gaffer.function.simple.filter;

import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import gaffer.function.SingleInputFilterFunction;
import gaffer.function.annotation.Inputs;

@Inputs(String.class)
public class MultiRegex extends SingleInputFilterFunction {
    private Pattern[] patterns;

    public MultiRegex() {
        // Required for serialisations
    }

    public MultiRegex(final Pattern[] patterns) {
        this.patterns = patterns;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonProperty("value")
    public Pattern[] getPatterns() {
        return patterns;
    }

    public void setControlValue(final Pattern[] patterns) {
        this.patterns = patterns;
    }

    @Override
    protected boolean filter(final Object input) {
        if (null == input || input.getClass() != String.class) {
            return false;
        }
        for (Pattern pattern : patterns) {
            if (pattern.matcher((CharSequence) input).matches()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public MultiRegex statelessClone() {
        return new MultiRegex(patterns);
    }
}

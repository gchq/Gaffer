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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import gaffer.function.SimpleFilterFunction;
import gaffer.function.annotation.Inputs;

import java.util.Arrays;
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
    protected boolean _isValid(final String input) {
        if (null == input || input.getClass() != String.class) {
            return false;
        }
        for (Pattern pattern : patterns) {
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
}

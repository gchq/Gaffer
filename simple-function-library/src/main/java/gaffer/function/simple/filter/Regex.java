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

import java.util.regex.Pattern;

@Inputs(String.class)
public class Regex extends SimpleFilterFunction<String> {
    private Pattern controlValue;

    public Regex() {
        // Required for serialisation
    }

    public Regex(final String controlValue) {
        this(Pattern.compile(controlValue));
    }

    public Regex(final Pattern controlValue) {
        this.controlValue = controlValue;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonProperty("value")
    public Pattern getControlValue() {
        return controlValue;
    }

    public void setControlValue(final Pattern controlValue) {
        this.controlValue = controlValue;
    }

    @Override
    protected boolean _isValid(final String input) {
        return !(null == input || input.getClass() != String.class)
                && controlValue.matcher(input).matches();
    }

    @Override
    public Regex statelessClone() {
        return new Regex(controlValue);
    }
}

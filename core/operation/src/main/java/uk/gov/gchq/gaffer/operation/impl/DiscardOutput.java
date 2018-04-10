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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.koryphe.Since;

import java.util.Map;

/**
 * A {@code DiscardOutput} operation is used as a terminal operation to indicate
 * that the results from the previous operation are not used again.
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.0.0")
public class DiscardOutput implements
        Input<Object> {

    private Map<String, String> options;

    @Override
    public Object getInput() {
        return null;
    }

    @Override
    public void setInput(final Object input) {
        // No action required
    }

    @Override
    public DiscardOutput shallowClone() throws CloneFailedException {
        return new DiscardOutput.Builder()
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static final class Builder extends BaseBuilder<DiscardOutput, Builder>
            implements Input.Builder<DiscardOutput, Object, Builder> {
        public Builder() {
            super(new DiscardOutput());
        }
    }
}

/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.io;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.GenericInput;

import java.util.Collections;
import java.util.Map;

@JsonPropertyOrder(alphabetic = true)
public class GenericInputImpl extends GenericInput<Object> {
    public GenericInputImpl() {
        super();
    }

    public GenericInputImpl(final Object input) {
        super(input);
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        // cannot clone
        return null;
    }

    @Override
    public Map<String, String> getOptions() {
        return Collections.emptyMap();
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        // do nothing
    }
}

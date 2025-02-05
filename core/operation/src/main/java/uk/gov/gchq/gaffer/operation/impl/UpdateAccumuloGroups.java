/*
 * Copyright 2016-2020 Crown Copyright
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
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code Validate} operation takes in {@link uk.gov.gchq.gaffer.data.element.Element}s validates them using the
 * store schema and returns the valid {@link uk.gov.gchq.gaffer.data.element.Element}s.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.UpdateGroups.Builder
 */
@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("2.4.0")
@Summary("Updates Groups based on the schema")
public class UpdateAccumuloGroups implements Output<void>{
    private Map<String, String> options;

    @Override
    public UpdateAccumuloGroups shallowClone() throws FailedCloneException {
        return new Builder()
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

    @Override
    public TypeReference<String> getOutputTypeReference() {
        return new TypeReferenceImpl.String();
    }

    public static final class Builder extends Operation.BaseBuilder<UpdateAccumuloGroups, Builder> implements Output.Builder<UpdateGroups, String>, Builder> {
        public Builder() {
            super(new UpdateAccumuloGroups());
        }
    }
}

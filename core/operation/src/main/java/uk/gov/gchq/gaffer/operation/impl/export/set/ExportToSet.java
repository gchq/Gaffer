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

package uk.gov.gchq.gaffer.operation.impl.export.set;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * An {@code ExportToSet} Export operation exports results to a Set.
 * This Set export is maintained per single Job or {@link uk.gov.gchq.gaffer.operation.OperationChain} only.
 * It cannot be used across multiple separate operation requests.
 * So ExportToSet and GetSetExport must be used inside a single operation chain.
 */
@JsonPropertyOrder(value = {"class", "input", "key"}, alphabetic = true)
@Since("1.0.0")
@Summary("Exports results to a Set")
public class ExportToSet<T> implements
        ExportTo<T> {
    private String key;
    private T input;
    private Map<String, String> options;

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(final String key) {
        this.key = key;
    }

    @Override
    public T getInput() {
        return input;
    }

    @Override
    public void setInput(final T input) {
        this.input = input;
    }

    @Override
    public TypeReference<T> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }

    @Override
    public ExportToSet<T> shallowClone() {
        return new ExportToSet.Builder<T>()
                .key(key)
                .input(input)
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

    public static final class Builder<T> extends Operation.BaseBuilder<ExportToSet<T>, Builder<T>>
            implements ExportTo.Builder<ExportToSet<T>, T, Builder<T>> {
        public Builder() {
            super(new ExportToSet<>());
        }
    }
}

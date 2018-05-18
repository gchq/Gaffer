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
package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.operation.io.AbstractIOOperation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

/**
 * A {@code Count} operation counts how many items there are in the provided {@link Iterable}.
 */
@JsonPropertyOrder(value = {"class", "input"}, alphabetic = true)
@Since("1.0.0")
@Summary("Counts the number of items")
public class Count<T> extends AbstractIOOperation<Count, T> implements
        InputOutput<Iterable<? extends T>, Long>,
        MultiInput<T> {


    @Override
    public TypeReference<Long> getOutputTypeReference() {
        return new TypeReferenceImpl.Long();
    }

    @Override
    public Count shallowClone() {
        return (Count<T>) new Count<T>()
                .input(getInput())
                .options(this.options);
    }

}

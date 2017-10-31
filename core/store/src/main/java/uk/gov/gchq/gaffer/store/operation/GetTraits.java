/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreTrait;

import java.util.Map;

/**
 * An Operation used for getting traits from the Store.
 */
public class GetTraits implements Operation, Output<Iterable<? extends StoreTrait>> {

    public static final boolean defaultCurrentlyAvailableTraits = true;
    private boolean currentlyAvailableTraits = defaultCurrentlyAvailableTraits;
    private Map<String, String> options;

    public boolean currentlyAvailableTraits() {
        return currentlyAvailableTraits;
    }

    public void setCurrentlyAvailableTraits(final boolean currentlyAvailableTraits) {
        this.currentlyAvailableTraits = currentlyAvailableTraits;
    }

    @Override
    public GetTraits shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .currentlyAvailableTraits(currentlyAvailableTraits)
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
    public TypeReference<Iterable<? extends StoreTrait>> getOutputTypeReference() {
        return new IterableStoreTrait();
    }

    public static class Builder extends BaseBuilder<GetTraits, Builder> {
        public Builder() {
            super(new GetTraits());
        }

        public Builder currentlyAvailableTraits(final boolean currentlyAvailableTraits) {
            _getOp().setCurrentlyAvailableTraits(currentlyAvailableTraits);
            return _self();
        }
    }

    public static class IterableStoreTrait extends TypeReference<Iterable<? extends StoreTrait>> {
    }
}

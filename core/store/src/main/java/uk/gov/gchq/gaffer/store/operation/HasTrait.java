/*
 * Copyright 2022 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.HashMap;
import java.util.Map;

/**
 * An Operation that will see if a Store has a given trait.
 */
@JsonPropertyOrder(alphabetic = true)
@Since("2.0.0")
@Summary("An Operation that will see if a Store has a given trait")
public class HasTrait implements Operation, Output<Boolean> {
    public static final boolean DEFAULT_CURRENT_TRAITS = true;

    /**
     * The currentTraits holds a boolean value, and is used
     * to check if the provided trait argument exists in either
     * the store default (false) or the schema current traits (true).
     * By default, it will check against the list of current traits.
     */
    private boolean currentTraits = DEFAULT_CURRENT_TRAITS;
    private StoreTrait trait;
    private Map<String, String> options = new HashMap<>();

    @Override
    public HasTrait shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .currentTraits(currentTraits)
                .trait(trait)
                .build();
    }

    public boolean isCurrentTraits() {
        return currentTraits;
    }

    public void setCurrentTraits(final boolean currentTraits) {
        this.currentTraits = currentTraits;
    }

    public StoreTrait getTrait() {
        return trait;
    }

    public void setTrait(final StoreTrait trait) {
        this.trait = trait;
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
    public TypeReference<Boolean> getOutputTypeReference() {
        return new TypeReferenceImpl.Boolean();
    }

    public static class Builder extends BaseBuilder<HasTrait, Builder> {
        public Builder() {
            super(new HasTrait());
        }

        public Builder currentTraits(final boolean currentTraits) {
            _getOp().setCurrentTraits(currentTraits);
            return _self();
        }

        public Builder trait(final StoreTrait trait) {
            _getOp().setTrait(trait);
            return _self();
        }
    }
}

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
package gaffer.operation.simple;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractOperation;
import gaffer.operation.VoidOutput;
import gaffer.operation.data.ElementSeed;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * An <code>UpdateStore</code> operation will transform elements that
 * match the provided {@link ElementSeed}s. If no seeds are provided all elements
 * that match the view will be updated.
 * A view should be provided with the appropriate transform functions.
 *
 * @see UpdateElements.Builder
 */
public class UpdateElements extends AbstractOperation<Iterable<ElementSeed>, Void> implements VoidOutput<Iterable<ElementSeed>> {
    public Iterable<ElementSeed> getSeeds() {
        return getInput();
    }

    public void setSeeds(final Iterable<ElementSeed> seeds) {
        setInput(seeds);
    }

    @JsonIgnore
    @Override
    public Iterable<ElementSeed> getInput() {
        return super.getInput();
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonGetter(value = "seeds")
    List<ElementSeed> getSeedArray() {
        final Iterable<ElementSeed> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    @JsonSetter(value = "seeds")
    void setSeedArray(final ElementSeed[] seeds) {
        setInput(Arrays.asList(seeds));
    }

    public static class Builder extends AbstractOperation.Builder<UpdateElements, Iterable<ElementSeed>, Void> {
        private List<ElementSeed> seeds;

        public Builder() {
            super(new UpdateElements());
        }

        /**
         * Sets an {@link Iterable} of ElementSeed on the operation.
         * It should not be used in conjunction with addSeed(ElementSeed).
         *
         * @param newSeeds an {@link Iterable} of ElementSeed to set on the operation.
         * @return this Builder
         */
        public Builder seeds(final Iterable<ElementSeed> newSeeds) {
            if (null != seeds) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            op.setSeeds(newSeeds);
            return this;
        }

        /**
         * Adds a single ElementSeed to a {@link LinkedList} of seeds on the operation.
         * It should not be used in conjunction with seeds(Iterable).
         *
         * @param seed a single ElementSeed to add to a {@link LinkedList} of seeds on the operation.
         * @return this Builder
         */
        public Builder addSeed(final ElementSeed seed) {
            if (null == seeds) {
                if (null != op.getSeeds()) {
                    throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
                }
                seeds = new LinkedList<>();
                op.setInput(seeds);
            } else if (seeds != op.getSeeds()) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }

            seeds.add(seed);
            return this;
        }

        @Override
        public Builder view(final View view) {
            return (Builder) super.view(view);
        }

        @Override
        public Builder option(final String name, final String value) {
            return (Builder) super.option(name, value);
        }
    }
}

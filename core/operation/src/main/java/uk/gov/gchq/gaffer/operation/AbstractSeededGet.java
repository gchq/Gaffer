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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractSeededGet<I_ITEM, O>
        extends AbstractGet<CloseableIterable<I_ITEM>, O> implements SeededGet<I_ITEM, O> {
    @Override
    public CloseableIterable<I_ITEM> getSeeds() {
        return getInput();
    }

    public void setSeeds(final Iterable<I_ITEM> seeds) {
        setSeeds(new WrappedCloseableIterable<>(seeds));
    }

    @Override
    public void setSeeds(final CloseableIterable<I_ITEM> seeds) {
        setInput(seeds);
    }

    @JsonIgnore
    @Override
    public CloseableIterable<I_ITEM> getInput() {
        return super.getInput();
    }

    @JsonProperty
    @Override
    public void setInput(final CloseableIterable<I_ITEM> input) {
        super.setInput(input);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter(value = "seeds")
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "if the iterable is null then the array should be null")
    public I_ITEM[] getSeedArray() {
        final CloseableIterable<I_ITEM> input = getInput();
        return null != input ? (I_ITEM[]) Lists.newArrayList(input).toArray() : null;
    }

    @JsonSetter(value = "seeds")
    void setSeedArray(final I_ITEM[] seeds) {
        setInput(new WrappedCloseableIterable<>(Arrays.asList(seeds)));
    }

    public abstract static class BaseBuilder<
            OP_TYPE extends AbstractSeededGet<I_ITEM, O>,
            I_ITEM,
            O,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, I_ITEM, O, ?>
            >
            extends AbstractGet.BaseBuilder<OP_TYPE, CloseableIterable<I_ITEM>, O, CHILD_CLASS> {

        protected List<I_ITEM> seeds;

        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        /**
         * Builds the operation and returns it.
         *
         * @return the built operation.
         */
        public OP_TYPE build() {
            if (null == op.getSeeds()) {
                if (seeds != null) {
                    op.setSeeds(seeds);
                }
            }
            return op;
        }

        /**
         * Sets an {@link java.lang.Iterable} of seed on the operation.
         * It should not be used in conjunction with addSeed(I_ITEM).
         *
         * @param newSeeds an {@link java.lang.Iterable} of seed to set on the operation.
         * @return this Builder
         */
        public CHILD_CLASS seeds(final Iterable<I_ITEM> newSeeds) {
            if (null != seeds) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            op.setSeeds(newSeeds);
            return self();
        }

        /**
         * Sets an {@link CloseableIterable} of seed on the operation.
         * It should not be used in conjunction with addSeed(I_ITEM).
         *
         * @param newSeeds an {@link CloseableIterable} of seed to set on the operation.
         * @return this Builder
         */
        public CHILD_CLASS seeds(final CloseableIterable<I_ITEM> newSeeds) {
            if (null != seeds) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            op.setSeeds(newSeeds);
            return self();
        }

        /**
         * Adds a single seed to a {@link java.util.LinkedList} of seeds on the operation.
         * It should not be used in conjunction with seeds(Iterable).
         *
         * @param seed a single seed to add to a {@link java.util.LinkedList} of seeds on the operation.
         * @return this Builder
         */
        public CHILD_CLASS addSeed(final I_ITEM seed) {
            if (null != op.getSeeds()) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            if (null == seeds) {
                seeds = new LinkedList<>();
            }
            seeds.add(seed);
            return self();
        }
    }
}

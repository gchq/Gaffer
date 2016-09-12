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

package gaffer.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.WrappedCloseableIterable;
import gaffer.data.element.Edge;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractGetOperation<SEED_TYPE, RESULT_TYPE>
        extends AbstractOperation<CloseableIterable<SEED_TYPE>, CloseableIterable<RESULT_TYPE>> implements GetOperation<SEED_TYPE, RESULT_TYPE> {
    private boolean includeEntities = true;
    private IncludeEdgeType includeEdges = IncludeEdgeType.ALL;
    private IncludeIncomingOutgoingType includeIncomingOutGoing = IncludeIncomingOutgoingType.BOTH;
    private SeedMatchingType seedMatching = SeedMatchingType.RELATED;
    private boolean populateProperties = true;
    private boolean deduplicate = false;

    protected AbstractGetOperation() {
        super();
    }

    protected AbstractGetOperation(final Iterable<SEED_TYPE> seeds) {
        this(new WrappedCloseableIterable<>(seeds));
    }

    protected AbstractGetOperation(final CloseableIterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    protected AbstractGetOperation(final View view) {
        super(view);
    }

    protected AbstractGetOperation(final View view, final Iterable<SEED_TYPE> seeds) {
        this(view, new WrappedCloseableIterable<SEED_TYPE>(seeds));
    }

    protected AbstractGetOperation(final View view, final CloseableIterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    protected AbstractGetOperation(final GetOperation<SEED_TYPE, ?> operation) {
        super(operation);
        setPopulateProperties(operation.isPopulateProperties());
        setIncludeEdges(operation.getIncludeEdges());
        setIncludeEntities(operation.isIncludeEntities());
        setSeedMatching(operation.getSeedMatching());
    }

    /**
     * @param seedMatching a {@link gaffer.operation.GetOperation.SeedMatchingType} describing how the seeds should be
     *                     matched to the identifiers in the graph.
     * @see gaffer.operation.GetOperation.SeedMatchingType
     */
    protected void setSeedMatching(final SeedMatchingType seedMatching) {
        this.seedMatching = seedMatching;
    }

    @Override
    public SeedMatchingType getSeedMatching() {
        return seedMatching;
    }

    @Override
    public CloseableIterable<SEED_TYPE> getSeeds() {
        return getInput();
    }

    public void setSeeds(final Iterable<SEED_TYPE> seeds) {
        setSeeds(new WrappedCloseableIterable<>(seeds));
    }

    @Override
    public void setSeeds(final CloseableIterable<SEED_TYPE> seeds) {
        setInput(seeds);
    }

    @JsonIgnore
    @Override
    public CloseableIterable<SEED_TYPE> getInput() {
        return super.getInput();
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonGetter(value = "seeds")
    List<SEED_TYPE> getSeedArray() {
        final Iterable<SEED_TYPE> input = getInput();
        return null != input ? Lists.newArrayList(input) : null;
    }

    @JsonSetter(value = "seeds")
    void setSeedArray(final SEED_TYPE[] seeds) {
        setInput(new WrappedCloseableIterable<>(Arrays.asList(seeds)));
    }

    @Override
    public boolean validate(final Edge edge) {
        return validateFlags(edge) && super.validate(edge);
    }

    @Override
    public boolean validate(final Entity entity) {
        return validateFlags(entity) && super.validate(entity);
    }

    @Override
    public boolean validateFlags(final Entity entity) {
        return isIncludeEntities();
    }

    @Override
    public boolean validateFlags(final Edge edge) {
        return null != getIncludeEdges() && getIncludeEdges().accept(edge.isDirected());
    }

    @Override
    public boolean isIncludeEntities() {
        return includeEntities;
    }

    @Override
    public void setIncludeEntities(final boolean includeEntities) {
        this.includeEntities = includeEntities;
    }

    @Override
    public IncludeIncomingOutgoingType getIncludeIncomingOutGoing() {
        return includeIncomingOutGoing;
    }

    @Override
    public void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType includeIncomingOutGoing) {
        this.includeIncomingOutGoing = includeIncomingOutGoing;
    }

    @Override
    public void setIncludeEdges(final IncludeEdgeType includeEdges) {
        this.includeEdges = includeEdges;
    }

    @Override
    public IncludeEdgeType getIncludeEdges() {
        return includeEdges;
    }

    @Override
    public boolean isPopulateProperties() {
        return populateProperties;
    }

    @Override
    public void setPopulateProperties(final boolean populateProperties) {
        this.populateProperties = populateProperties;
    }

    @Override
    public boolean isDeduplicate() {
        return deduplicate;
    }

    @Override
    public void setDeduplicate(final boolean deduplicate) {
        this.deduplicate = deduplicate;
    }

    public static class Builder<OP_TYPE extends AbstractGetOperation<SEED_TYPE, RESULT_TYPE>, SEED_TYPE, RESULT_TYPE>
            extends AbstractOperation.Builder<OP_TYPE, CloseableIterable<SEED_TYPE>, CloseableIterable<RESULT_TYPE>> {
        private List<SEED_TYPE> seeds;

        protected Builder(final OP_TYPE op) {
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
         * Sets an {@link java.lang.Iterable} of SEED_TYPE on the operation.
         * It should not be used in conjunction with addSeed(SEED_TYPE).
         *
         * @param newSeeds an {@link java.lang.Iterable} of SEED_TYPE to set on the operation.
         * @return this Builder
         */
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> seeds(final Iterable<SEED_TYPE> newSeeds) {
            if (null != seeds) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            op.setSeeds(newSeeds);
            return this;
        }

        /**
         * Sets an {@link CloseableIterable} of SEED_TYPE on the operation.
         * It should not be used in conjunction with addSeed(SEED_TYPE).
         *
         * @param newSeeds an {@link CloseableIterable} of SEED_TYPE to set on the operation.
         * @return this Builder
         */
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> seeds(final CloseableIterable<SEED_TYPE> newSeeds) {
            if (null != seeds) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            op.setSeeds(newSeeds);
            return this;
        }

        /**
         * Adds a single SEED_TYPE to a {@link java.util.LinkedList} of seeds on the operation.
         * It should not be used in conjunction with seeds(Iterable).
         *
         * @param seed a single SEED_TYPE to add to a {@link java.util.LinkedList} of seeds on the operation.
         * @return this Builder
         */
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> addSeed(final SEED_TYPE seed) {
            if (null != op.getSeeds()) {
                throw new IllegalStateException("Either use builder method 'seeds' or 'addSeed' you cannot use both");
            }
            if (null == seeds) {
                seeds = new LinkedList<>();
            }
            seeds.add(seed);
            return this;
        }

        /**
         * @param includeEntities sets the includeEntities flag on the operation.
         * @return this Builder
         * @see gaffer.operation.GetOperation#setIncludeEntities(boolean)
         */
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> includeEntities(final boolean includeEntities) {
            op.setIncludeEntities(includeEntities);
            return this;
        }

        /**
         * @param includeEdgeType sets the includeEdges option on the operation.
         * @return this Builder
         * @see gaffer.operation.GetOperation#setIncludeEdges(IncludeEdgeType)
         */
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> includeEdges(final IncludeEdgeType includeEdgeType) {
            op.setIncludeEdges(includeEdgeType);
            return this;
        }

        /**
         * @param inOutType sets the includeIncomingOutGoing option on the operation.
         * @return this Builder
         * @see gaffer.operation.GetOperation#setIncludeIncomingOutGoing(IncludeIncomingOutgoingType)
         */
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> inOutType(final IncludeIncomingOutgoingType inOutType) {
            op.setIncludeIncomingOutGoing(inOutType);
            return this;
        }

        /**
         * @param deduplicate sets the deduplicate flag on the operation.
         * @return this Builder
         * @see gaffer.operation.GetOperation#setDeduplicate(boolean)
         */
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> deduplicate(final boolean deduplicate) {
            op.setDeduplicate(deduplicate);
            return this;
        }

        /**
         * @param populateProperties set the populateProperties flag on the operation.
         * @return this Builder
         * @see gaffer.operation.GetOperation#setPopulateProperties(boolean)
         */
        protected Builder<OP_TYPE, SEED_TYPE, RESULT_TYPE> populateProperties(final boolean populateProperties) {
            op.setPopulateProperties(populateProperties);
            return this;
        }
    }
}

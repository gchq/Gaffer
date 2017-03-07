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

package uk.gov.gchq.gaffer.operation.impl.get;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.AbstractGetIterableOperation;
import uk.gov.gchq.gaffer.operation.ElementOperation;
import uk.gov.gchq.gaffer.operation.GetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Collections;
import java.util.List;

/**
 * An <code>GetAdjacentEntitySeeds</code> operation will return the
 * {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s at the opposite end of connected edges to a seed
 * {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}.
 *
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds.Builder
 * @see uk.gov.gchq.gaffer.operation.GetIterableOperation
 */
public class GetAdjacentEntitySeeds
        extends AbstractGetIterableOperation<EntitySeed, EntitySeed>
        implements ElementOperation<CloseableIterable<EntitySeed>, CloseableIterable<EntitySeed>> {
    private DirectedType directedType = DirectedType.BOTH;
    private IncludeIncomingOutgoingType includeIncomingOutGoing = IncludeIncomingOutgoingType.BOTH;

    public GetAdjacentEntitySeeds() {
    }

    public GetAdjacentEntitySeeds(final Iterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetAdjacentEntitySeeds(final CloseableIterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetAdjacentEntitySeeds(final View view) {
        super(view);
    }

    public GetAdjacentEntitySeeds(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetAdjacentEntitySeeds(final View view, final CloseableIterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetAdjacentEntitySeeds(final GetIterableElementsOperation<EntitySeed, ?> operation) {
        super(operation);
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
    public void setDirectedType(final DirectedType directedType) {
        this.directedType = directedType;
    }

    @Override
    public DirectedType getDirectedType() {
        return directedType;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @JsonGetter(value = "seeds")
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "if the iterable is null then the array should be null")
    @Override
    public EntitySeed[] getSeedArray() {
        final CloseableIterable<EntitySeed> input = getInput();
        if (null != input) {
            final List<EntitySeed> inputList = Lists.newArrayList(input);
            return inputList.toArray(new EntitySeed[inputList.size()]);
        }

        return null;
    }

    @Override
    public void setView(final View view) {
        if (null != view && view.hasEntities()) {
            super.setView(new View.Builder()
                    .merge(view)
                    .entities(Collections.emptyMap())
                    .build());
        } else {
            super.setView(view);
        }
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableEntitySeed();
    }

    @Override
    public boolean validate(final Edge edge) {
        return null != edge && validateFlags(edge) && super.validate(edge);
    }

    public boolean validateFlags(final Edge edge) {
        return DirectedType.BOTH == getDirectedType()
                || (DirectedType.DIRECTED == getDirectedType() && edge.isDirected())
                || (DirectedType.UNDIRECTED == getDirectedType() && !edge.isDirected());
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetIterableOperation.BaseBuilder<GetAdjacentEntitySeeds, EntitySeed, EntitySeed, CHILD_CLASS> {
        public BaseBuilder() {
            super(new GetAdjacentEntitySeeds());
        }

        /**
         * @param directedType sets the directedType option on the operation.
         * @return this Builder
         */
        public CHILD_CLASS directedType(final DirectedType directedType) {
            op.setDirectedType(directedType);
            return self();
        }

        /**
         * @param inOutType sets the includeIncomingOutGoing option on the operation.
         * @return this Builder
         */
        public CHILD_CLASS inOutType(final IncludeIncomingOutgoingType inOutType) {
            op.setIncludeIncomingOutGoing(inOutType);
            return self();
        }

    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}

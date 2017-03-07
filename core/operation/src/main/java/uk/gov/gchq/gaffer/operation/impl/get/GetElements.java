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
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.AbstractSeededGraphGetIterable;
import java.util.List;

/**
 * Restricts {@link AbstractSeededGraphGetIterable} to take {@link uk.gov.gchq.gaffer.data.element.id.ElementId}s as
 * seeds and returns {@link uk.gov.gchq.gaffer.data.element.Element}s
 * There are various flags to filter out the elements returned. See implementations of {@link GetElements} for further details.
 *
 * @param <ID>           the id type
 * @param <ELEMENT_TYPE> the element return type
 */
public class GetElements<ID extends ElementId, ELEMENT_TYPE extends Element>
        extends AbstractSeededGraphGetIterable<ID, ELEMENT_TYPE>
        implements SeedMatching {
    private SeedMatchingType seedMatching = SeedMatchingType.RELATED;

    /**
     * @param seedMatching a {@link SeedMatchingType} describing how the seeds should be
     *                     matched to the identifiers in the graph.
     * @see SeedMatchingType
     */
    public void setSeedMatching(final SeedMatchingType seedMatching) {
        this.seedMatching = seedMatching;
    }

    public SeedMatchingType getSeedMatching() {
        return seedMatching;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @JsonGetter(value = "seeds")
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "if the iterable is null then the array should be null")
    @Override
    public ID[] getSeedArray() {
        final CloseableIterable<ID> input = getInput();
        if (null != input) {
            final List<ID> inputList = Lists.newArrayList(input);
            return (ID[]) inputList.toArray(new ElementId[inputList.size()]);
        }

        return null;
    }

    public abstract static class BaseBuilder<OP_TYPE extends GetElements<ID, ELEMENT_TYPE>,
            ID extends ElementId,
            ELEMENT_TYPE extends Element,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, ID, ELEMENT_TYPE, ?>>
            extends AbstractSeededGraphGetIterable.BaseBuilder<OP_TYPE, ID, ELEMENT_TYPE, CHILD_CLASS> {
        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }

        protected BaseBuilder() {
            super((OP_TYPE) new GetElements<ID, ELEMENT_TYPE>());
        }

        public CHILD_CLASS seedMatching(final SeedMatchingType seedMatching) {
            op.setSeedMatching(seedMatching);
            return self();
        }
    }

    public static final class Builder<ID extends ElementId, ELEMENT_TYPE extends Element>
            extends BaseBuilder<GetElements<ID, ELEMENT_TYPE>, ID, ELEMENT_TYPE, Builder<ID, ELEMENT_TYPE>> {

        public Builder() {
            super(new GetElements<>());
        }

        public Builder(final GetElements<ID, ELEMENT_TYPE> op) {
            super(op);
        }

        @Override
        protected Builder<ID, ELEMENT_TYPE> self() {
            return this;
        }
    }
}

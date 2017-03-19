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
package uk.gov.gchq.gaffer.accumulostore.operation.impl;


import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.AbstractGetIterableElementsOperation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;

/**
 * A <code>SummariseGroupOverRanges</code> operation will return an
 * {@link uk.gov.gchq.gaffer.data.element.Element} that represents the aggregated form of all data between the provided range for the provided group.
 * Note that one result per tablet on which data in the desired range resides will be returned, with large data sets and/or large ranges
 * more likely to produce multiple results and you will need to cache the results and aggregate them again to get a final answer.
 * For this reason it is recommended your provided ranges do not over-lap as you will be unable to tell for a given result which range the result is from.
 * Standard filtering will still occur before the final aggregation of the vertices.
 *
 * @see uk.gov.gchq.gaffer.operation.GetOperation
 */
public class SummariseGroupOverRanges<SEED_TYPE extends Pair<? extends ElementSeed>, ELEMENT_TYPE extends Element> extends GetElementsInRanges<SEED_TYPE, ELEMENT_TYPE> {

    public SummariseGroupOverRanges() {
    }

    public SummariseGroupOverRanges(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public abstract static class BaseBuilder<SEED_TYPE extends Pair<? extends ElementSeed>, ELEMENT_TYPE extends Element, CHILD_CLASS extends BaseBuilder<SEED_TYPE, ELEMENT_TYPE, ?>>
            extends AbstractGetIterableElementsOperation.BaseBuilder<SummariseGroupOverRanges<SEED_TYPE, ELEMENT_TYPE>, SEED_TYPE, ELEMENT_TYPE, CHILD_CLASS> {
        public BaseBuilder() {
            super(new SummariseGroupOverRanges<SEED_TYPE, ELEMENT_TYPE>());
        }
    }

    public static final class Builder<SEED_TYPE extends Pair<? extends ElementSeed>, ELEMENT_TYPE extends Element>
        extends BaseBuilder<SEED_TYPE, ELEMENT_TYPE, Builder<SEED_TYPE, ELEMENT_TYPE>> {

        @Override
        protected Builder<SEED_TYPE, ELEMENT_TYPE> self() {
            return this;
        }
    }
}

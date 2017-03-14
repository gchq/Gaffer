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
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.graph.AbstractSeededGraph;

/**
 * A <code>SummariseGroupOverRanges</code> operation will return an
 * {@link uk.gov.gchq.gaffer.data.element.Element} that represents the aggregated form of all data between the provided range for the provided group.
 * Note that one result per tablet on which data in the desired range resides will be returned, with large data sets and/or large ranges
 * more likely to produce multiple results and you will need to cache the results and aggregate them again to get a final answer.
 * For this reason it is recommended your provided ranges do not over-lap as you will be unable to tell for a given result which range the result is from.
 * Standard filtering will still occur before the final aggregation of the vertices.
 */
public class SummariseGroupOverRanges<I_ITEM extends Pair<? extends ElementSeed>, E extends Element> extends GetElementsInRanges<I_ITEM, E> {
    public abstract static class BaseBuilder<I_ITEM extends Pair<? extends ElementSeed>, E extends Element, CHILD_CLASS extends BaseBuilder<I_ITEM, E, ?>>
            extends AbstractSeededGraph.BaseBuilder<SummariseGroupOverRanges<I_ITEM, E>, I_ITEM, E, CHILD_CLASS> {
        public BaseBuilder() {
            super(new SummariseGroupOverRanges<>());
        }
    }

    public static final class Builder<I_ITEM extends Pair<? extends ElementSeed>, E extends Element>
            extends BaseBuilder<I_ITEM, E, Builder<I_ITEM, E>> {

        @Override
        protected Builder<I_ITEM, E> self() {
            return this;
        }
    }
}

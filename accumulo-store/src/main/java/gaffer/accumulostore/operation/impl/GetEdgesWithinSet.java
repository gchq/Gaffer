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

package gaffer.accumulostore.operation.impl;

import gaffer.data.element.Edge;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import gaffer.operation.data.EntitySeed;

/**
 * Returns {@link gaffer.data.element.Edge}s where both ends are in a given set.
 *
 **/
public class GetEdgesWithinSet extends GetElementsWithinSet<Edge> {

    public GetEdgesWithinSet(final Iterable<EntitySeed> seeds) {
        super(seeds);
    }

    public GetEdgesWithinSet(final View view) {
        super(view);
    }

    public GetEdgesWithinSet(final View view, final Iterable<EntitySeed> seeds) {
        super(view, seeds);
    }

    public GetEdgesWithinSet(final GetOperation<EntitySeed, ?> operation) {
        super(operation);
    }

    public static class Builder<OP_TYPE extends GetEdgesWithinSet> extends GetElementsWithinSet.Builder<OP_TYPE, Edge> {

        protected Builder(final OP_TYPE op) {
            super(op);
        }

    }

}

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

package gaffer.operation.impl.get;

import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractGetOperation;
import gaffer.operation.GetOperation;
import gaffer.operation.data.ElementSeed;

/**
 * Restricts {@link gaffer.operation.AbstractGetOperation} to take {@link gaffer.operation.data.ElementSeed}s as
 * seeds and returns {@link gaffer.data.element.Element}s
 * There are various flags to filter out the elements returned. See implementations of {@link GetElements} for further details.
 *
 * @param <SEED_TYPE> the seed seed type
 * @param <ELEMENT_TYPE>  the element return type
 * @see gaffer.operation.GetOperation
 */
public abstract class GetElements<SEED_TYPE extends ElementSeed, ELEMENT_TYPE extends Element>
        extends AbstractGetOperation<SEED_TYPE, ELEMENT_TYPE> {
    public GetElements() {
        super();
    }

    public GetElements(final Iterable<SEED_TYPE> seeds) {
        super(seeds);
    }

    public GetElements(final View view) {
        super(view);
    }

    public GetElements(final View view, final Iterable<SEED_TYPE> seeds) {
        super(view, seeds);
    }

    public GetElements(final GetOperation<SEED_TYPE, ?> operation) {
        super(operation);
    }

    public static class Builder<OP_TYPE extends GetElements<SEED_TYPE, ELEMENT_TYPE>, SEED_TYPE extends ElementSeed, ELEMENT_TYPE extends Element>
            extends AbstractGetOperation.Builder<OP_TYPE, SEED_TYPE, ELEMENT_TYPE> {
        protected Builder(final OP_TYPE op) {
            super(op);
        }
    }

    public void setSeedMatching(final SeedMatchingType seedMatching) {
        super.setSeedMatching(seedMatching);
    }

}

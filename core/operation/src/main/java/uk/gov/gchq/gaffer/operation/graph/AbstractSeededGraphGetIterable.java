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

package uk.gov.gchq.gaffer.operation.graph;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.SeededGraphGetIterable;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public abstract class AbstractSeededGraphGetIterable<I_ITEM, O_ITEM>
        extends AbstractSeededGraphGet<I_ITEM, CloseableIterable<O_ITEM>> implements SeededGraphGetIterable<I_ITEM, O_ITEM> {
    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableElement();
    }

    public abstract static class BaseBuilder<
            OP_TYPE extends AbstractSeededGraphGetIterable<I_ITEM, O_ITEM>,
            I_ITEM,
            O_ITEM,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, I_ITEM, O_ITEM, ?>
            >
            extends AbstractSeededGraphGet.BaseBuilder<OP_TYPE, I_ITEM, CloseableIterable<O_ITEM>, CHILD_CLASS> {

        protected BaseBuilder(final OP_TYPE op) {
            super(op);
        }
    }
}

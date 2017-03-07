/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.export;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

/**
 * A <code>Export</code> allows the results of a previous operation in an
 * {@link uk.gov.gchq.gaffer.operation.OperationChain} to be exported, keyed on
 * an optional key. If a key is not provided the default key is 'ALL'.
 */
public abstract class Export extends ExportOperation<Object, Object> {
    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.Object();
    }

    public abstract static class BaseBuilder<EXPORT extends Export, CHILD_CLASS extends BaseBuilder<EXPORT, ?>>
            extends ExportOperation.BaseBuilder<EXPORT, Object, Object, CHILD_CLASS> {
        public BaseBuilder(final EXPORT export) {
            super(export);
        }
    }
}

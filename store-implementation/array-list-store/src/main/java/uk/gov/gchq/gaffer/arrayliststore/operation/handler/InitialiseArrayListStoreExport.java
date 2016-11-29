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

package uk.gov.gchq.gaffer.arrayliststore.operation.handler;

import uk.gov.gchq.gaffer.arrayliststore.export.ArrayListStoreExporter;
import uk.gov.gchq.gaffer.operation.impl.export.initialise.InitialiseExport;

public class InitialiseArrayListStoreExport extends InitialiseExport {
    public InitialiseArrayListStoreExport() {
        super(new ArrayListStoreExporter());
    }

    @Override
    public ArrayListStoreExporter getExporter() {
        return ((ArrayListStoreExporter) super.getExporter());
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends InitialiseExport.BaseBuilder<InitialiseArrayListStoreExport, CHILD_CLASS> {
        public BaseBuilder() {
            super(new InitialiseArrayListStoreExport());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}

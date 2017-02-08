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

package uk.gov.gchq.gaffer.operation.impl.export.initialise;

import uk.gov.gchq.gaffer.export.SetExporter;

public class InitialiseSetExport extends InitialiseExport {
    public InitialiseSetExport() {
        super(new SetExporter());
    }

    public InitialiseSetExport(final String key) {
        super(new SetExporter(), key);
    }

    @Override
    public SetExporter getExporter() {
        return ((SetExporter) super.getExporter());
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends InitialiseExport.BaseBuilder<InitialiseSetExport, CHILD_CLASS> {
        public BaseBuilder() {
            super(new InitialiseSetExport());
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}

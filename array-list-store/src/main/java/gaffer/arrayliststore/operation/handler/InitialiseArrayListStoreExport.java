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

package gaffer.arrayliststore.operation.handler;

import gaffer.arrayliststore.export.ArrayListStoreExporter;
import gaffer.operation.impl.export.initialise.InitialiseExport;

public class InitialiseArrayListStoreExport extends InitialiseExport {
    public InitialiseArrayListStoreExport() {
        super(new ArrayListStoreExporter());
    }

    @Override
    public ArrayListStoreExporter getExporter() {
        return ((ArrayListStoreExporter) super.getExporter());
    }

    public static class Builder extends InitialiseExport.Builder<InitialiseArrayListStoreExport> {
        public Builder() {
            super(new InitialiseArrayListStoreExport());
        }
    }
}

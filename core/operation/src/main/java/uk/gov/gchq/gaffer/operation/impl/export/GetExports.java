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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.VoidInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A <code>GetExports</code> operation gets multiple exports and returns then
 * in a Map.
 * The keys in the map are: "[ExportOperationClassName]: [key]"
 * The values in the map are the exported values.
 */
public class GetExports extends AbstractOperation<Void, Map<String, CloseableIterable<?>>> implements VoidInput<Map<String, CloseableIterable<?>>> {
    private List<GetExport> getExports = new ArrayList<>();

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public List<GetExport> getGetExports() {
        return getExports;
    }

    public void setGetExports(final List<GetExport> getExports) {
        if (null == getExports) {
            this.getExports = new ArrayList<>();
        } else {
            this.getExports = getExports;
        }
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.MapStringSet();
    }

    public abstract static class BaseBuilder<EXPORT extends GetExports, CHILD_CLASS extends BaseBuilder<EXPORT, CHILD_CLASS>>
            extends AbstractOperation.BaseBuilder<GetExports, Void, Map<String, CloseableIterable<?>>, CHILD_CLASS> {
        public BaseBuilder(final GetExports export) {
            super(export);
        }

        public CHILD_CLASS exports(final List<GetExport> exports) {
            getOp().setGetExports(exports);
            return self();
        }

        public CHILD_CLASS exports(final GetExport... exports) {
            getOp().getGetExports().clear();
            Collections.addAll(getOp().getGetExports(), exports);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<GetExports, Builder> {
        public Builder() {
            super(new GetExports());
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}

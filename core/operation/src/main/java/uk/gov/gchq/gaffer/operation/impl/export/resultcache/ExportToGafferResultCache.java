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

package uk.gov.gchq.gaffer.operation.impl.export.resultcache;

import com.google.common.collect.Sets;
import uk.gov.gchq.gaffer.operation.impl.export.Export;
import java.util.Set;

/**
 * An <code>ExportToGafferResultCache</code> Export operation exports results into
 * a cache. The cache is backed by a simple Gaffer graph that can be configured.
 * The results can be of any type - as long as they are json serialisable.
 */
public class ExportToGafferResultCache extends Export {
    private Set<String> opAuths;

    public Set<String> getOpAuths() {
        return opAuths;
    }

    public void setOpAuths(final Set<String> opAuths) {
        this.opAuths = opAuths;
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends Export.BaseBuilder<ExportToGafferResultCache, CHILD_CLASS> {
        public BaseBuilder() {
            super(new ExportToGafferResultCache());
        }

        public CHILD_CLASS opAuths(final Set<String> opAuths) {
            getOp().setOpAuths(opAuths);
            return self();
        }

        public CHILD_CLASS opAuths(final String... opAuths) {
            getOp().setOpAuths(Sets.newHashSet(opAuths));
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}

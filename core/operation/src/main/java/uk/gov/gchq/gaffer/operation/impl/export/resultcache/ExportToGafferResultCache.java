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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.Export;
import java.util.Set;

/**
 * An <code>ExportToGafferResultCache</code> Export operation exports results into
 * a cache. The cache is backed by a simple Gaffer graph that can be configured.
 * The results can be of any type - as long as they are json serialisable.
 */
public class ExportToGafferResultCache implements Operation, Export {
    private String key;
    private Set<String> opAuths;

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(final String key) {
        this.key = key;
    }

    public Set<String> getOpAuths() {
        return opAuths;
    }

    public void setOpAuths(final Set<String> opAuths) {
        this.opAuths = opAuths;
    }

    public static final class Builder extends Operation.BaseBuilder<ExportToGafferResultCache, Builder>
            implements Export.Builder<ExportToGafferResultCache, Builder> {
        public Builder() {
            super(new ExportToGafferResultCache());
        }

        public Builder opAuths(final Set<String> opAuths) {
            _getOp().setOpAuths(opAuths);
            return _self();
        }

        public Builder opAuths(final String... opAuths) {
            _getOp().setOpAuths(Sets.newHashSet(opAuths));
            return _self();
        }
    }
}

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

package gaffer.export;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.WrappedClosableIterable;
import gaffer.user.User;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A <code>HashMapListExporter</code> is an in memory temporary {@link Exporter}.
 * The export is maintained per single {@link gaffer.operation.OperationChain} only.
 * It cannot be used across multiple separate operation requests.
 * So, it must be updated and fetched inside a single operation chain.
 */
public class HashMapListExporter extends Exporter {
    private Map<String, List<Object>> export = new HashMap<>();

    @Override
    public boolean initialise(final Object config, final User user) {
        final boolean isNew = super.initialise(config, user);
        if (isNew) {
            export = new HashMap<>();
        }

        return isNew;
    }

    @Override
    protected void _add(final String key, final Iterable<Object> values, final User user) {
        final List<Object> exportValues = export.get(key);
        if (null == exportValues) {
            export.put(key, Lists.newArrayList(values));
        } else {
            Iterables.addAll(exportValues, values);
        }
    }

    @Override
    protected CloseableIterable<Object> _get(final String key, final User user, final int start, final int end) {
        List<Object> results = export.get(key);
        if (null != results) {
            int endTruncated = end > results.size() ? results.size() : end;
            results = export.get(key).subList(start, endTruncated);
        }

        return new WrappedClosableIterable<>(results);
    }

    public Map<String, List<Object>> getExport() {
        return Collections.unmodifiableMap(export);
    }
}

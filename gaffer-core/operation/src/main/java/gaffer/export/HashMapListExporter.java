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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A <code>HashMapListExporter</code> is an in memory temporary {@link Exporter}
 * using a {@link HashMap}. The underlying exportMap field has a getter and setter
 * to allow it to be retriever and reused across operation chains.
 */
public class HashMapListExporter extends Exporter {
    private Map<String, List<Object>> exportMap = new HashMap<>();

    @Override
    public void initialise(final Object config, final User user) {
        super.initialise(config, user);
        exportMap = new HashMap<>();
    }

    @Override
    protected void _add(final String key, final Iterable<?> values, final User user) {
        final List<Object> exportValues = exportMap.get(key);
        if (null == exportValues) {
            exportMap.put(key, Lists.newArrayList(values));
        } else {
            Iterables.addAll(exportValues, values);
        }
    }

    @Override
    protected CloseableIterable<?> _get(final String key, final User user, final int start, final int end) {
        List<Object> results = exportMap.get(key);
        if (null != results) {
            int endTruncated = end > results.size() ? results.size() : end;
            results = exportMap.get(key).subList(start, endTruncated);
        }

        return new WrappedClosableIterable<>(results);
    }

    public Map<String, List<Object>> getExportMap() {
        return exportMap;
    }

    public void setExportMap(final Map<String, List<Object>> exportMap) {
        if (null == exportMap) {
            this.exportMap = new HashMap<>();
        } else {
            this.exportMap = exportMap;
        }
    }
}

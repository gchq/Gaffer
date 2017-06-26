/*
 * Copyright 2017. Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class is used to store a file-based index for each group, i.e. for each group it stores a set of
 * {@link SubIndex}s. Each {@link SubIndex} contains a path to a file that contains data for that group along with
 * minimum and maximum values of the indexed columns within that file. This allows queries for particular values
 * of the indexed columns to skip files that do not contain relevant data.
 */
public class Index {
    private final Map<String, SubIndex> index;

    public Index() {
        this.index = new HashMap<>();
    }

    public void add(final String group, final SubIndex subIndex) {
        if (index.containsKey(group)) {
            throw new IllegalArgumentException("Cannot overwrite an entry in an index (group was " + group + ")");
        }
        index.put(group, subIndex);
    }

    public Set<String> groupsIndexed() {
        return Collections.unmodifiableSet(index.keySet());
    }

    public SubIndex get(final String group) {
        return index.get(group);
    }

    public static class SubIndex {
        private static final Comparator<MinMaxPath> BY_PATH =
                (MinMaxPath mmp1, MinMaxPath mmp2) -> mmp1.getPath().compareTo(mmp2.getPath());
        private final SortedSet<MinMaxPath> minMaxPaths;

        public SubIndex() {
            this.minMaxPaths = new TreeSet<>(BY_PATH);
        }

        public boolean isEmpty() {
            return minMaxPaths.isEmpty();
        }

        public void add(final MinMaxPath minMaxPath) {
            minMaxPaths.add(minMaxPath);
        }

        public Iterator<MinMaxPath> getIterator() {
            return minMaxPaths.iterator();
        }
    }

    public static class MinMaxPath {
        private final Object[] min;
        private final Object[] max;
        private final String path;

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
                justification = "This method is only used in this package and users will not mutate the values returned.")
        public MinMaxPath(final Object[] min, final Object[] max, final String path) {
            this.min = min;
            this.max = max;
            this.path = path;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP",
                justification = "This method is only used in this package and users will not mutate the values returned.")
        public Object[] getMin() {
            return min;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP",
                justification = "This method is only used in this package and users will not mutate the values returned.")
        public Object[] getMax() {
            return max;
        }

        public String getPath() {
            return path;
        }
    }
}

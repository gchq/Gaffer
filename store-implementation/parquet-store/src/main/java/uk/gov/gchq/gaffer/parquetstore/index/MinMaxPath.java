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
package uk.gov.gchq.gaffer.parquetstore.index;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.store.StoreException;
import java.util.Arrays;

/**
 * This class is used to store a single parquet file's path along with the Min and Max parquet objects that are contained in that file
 * for a specific gaffer column, i.e. if the vertex was a {@link uk.gov.gchq.gaffer.types.TypeValue} object then the Min would consist of two Strings,
 * the first representing the type and the second representing the value.
 */
public class MinMaxPath {
    private final Object[] min;
    private final Object[] max;
    private final String path;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
            justification = "This method is only used in this package and users will not mutate the values returned.")
    public MinMaxPath(final Object[] min, final Object[] max, final String path) throws StoreException {
        this.min = min;
        this.max = max;
        this.path = path;
        // run checks
        if (min.length != max.length) {
            throw new StoreException("The MinMaxPath " + this.toString() + " does not have equal length Min and Max arrays.");
        }
        for (int colIndex = 0; colIndex < min.length; colIndex++) {
            if (!min.getClass().getCanonicalName().equals(max.getClass().getCanonicalName())) {
                throw new StoreException("The MinMaxPath " + this.toString() + " should have the same class type at min[" + colIndex + "] and max[" + colIndex + "].");
            }
        }
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MinMaxPath{ ")
                .append("Min=").append(Arrays.toString(min))
                .append(", Max=").append(Arrays.toString(max))
                .append(", path=").append(path);
        return sb.toString();
    }
}

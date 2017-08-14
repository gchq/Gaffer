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
 * This class is used to store a single parquet file's path along with the first rows parquet objects that are contained in that file
 * for a specific gaffer column, i.e. if the vertex was a {@link uk.gov.gchq.gaffer.types.TypeValue} object then the Min would consist of two Strings,
 * the first representing the type and the second representing the value.
 */
public class MinValuesWithPath {
    private final Object[] min;
    private final String path;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
            justification = "This method is only used in this package and users will not mutate the values returned.")
    public MinValuesWithPath(final Object[] min, final String path) throws StoreException {
        this.min = min;
        this.path = path;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",
            justification = "This method is only used in this package and users will not mutate the values returned.")
    public Object[] getMin() {
        return min;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MinValuesWithPath{ ")
                .append("Min=").append(Arrays.toString(min))
                .append(", path=").append(path);
        return sb.toString();
    }
}

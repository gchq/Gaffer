/*
 * Copyright 2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.partitioner;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

public class PositiveInfinityPartitionKey extends PartitionKey {

    public PositiveInfinityPartitionKey() {
        super(null);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).toString();
    }

    @Override
    public int compareTo(final PartitionKey other) {
        if (other instanceof PositiveInfinityPartitionKey) {
            return 0;
        }
        return 1;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj || obj instanceof PositiveInfinityPartitionKey) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Integer.MAX_VALUE;
    }
}

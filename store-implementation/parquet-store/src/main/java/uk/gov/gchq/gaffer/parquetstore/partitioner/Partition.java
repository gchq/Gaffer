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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

/**
 * A <code>Partition</code> contains an integer id, and a minimum and maximum {@link PartitionKey}.
 */
public class Partition {
    private int partitionId;
    private final PartitionKey min;
    private final PartitionKey max;

    public Partition(final int partitionId, final PartitionKey min, final PartitionKey max) {
        this.partitionId = partitionId;
        if (null == min || null == max) {
            throw new IllegalArgumentException("The minimum and maximum partition keys must be non-null");
        }
        if (min.compareTo(max) > 0) {
            throw new IllegalArgumentException("The minimum partition key must be less than or equal to the maximum partition key");
        }
        this.min = min;
        this.max = max;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public PartitionKey getMin() {
        return min;
    }

    public PartitionKey getMax() {
        return max;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("partitionId", partitionId)
                .append("min", min)
                .append("max", max)
                .toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final Partition other = (Partition) obj;

        return new EqualsBuilder()
                .append(partitionId, other.partitionId)
                .append(min, other.min)
                .append(max, other.max)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(partitionId)
                .append(min)
                .append(max)
                .toHashCode();
    }
}

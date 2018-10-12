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
 * A <code>PartitionKey</code> is an array of <code>Object</code>s corresponding to the columns of a partition.
 */
public class PartitionKey implements Comparable<PartitionKey> {
    private final Object[] partitionKey;

    public PartitionKey(final Object[] partitionKey) {
        this.partitionKey = partitionKey;
    }

    public Object[] getPartitionKey() {
        return partitionKey;
    }

    public int getLength() {
        return partitionKey.length;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("partitionKey", partitionKey)
                .toString();
    }

    @Override
    public int compareTo(final PartitionKey other) {
        if (other instanceof NegativeInfinityPartitionKey) {
            return 1;
        }
        if (other instanceof PositiveInfinityPartitionKey) {
            return -1;
        }
        if (this.partitionKey.length != other.partitionKey.length) {
            throw new RuntimeException("Arrays should be of equal length: this.partitionKey.length = "
                    + this.partitionKey.length + ", other.partitionKey.length = " + other.partitionKey.length);
        }
        for (int i = 0; i < this.partitionKey.length; i++) {
            final int diff = ((Comparable) this.partitionKey[i]).compareTo((Comparable) other.partitionKey[i]);
            if (0 != diff) {
                return diff;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final PartitionKey other = (PartitionKey) obj;

        return new EqualsBuilder()
                .append(partitionKey, other.partitionKey)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(partitionKey)
                .toHashCode();
    }
}

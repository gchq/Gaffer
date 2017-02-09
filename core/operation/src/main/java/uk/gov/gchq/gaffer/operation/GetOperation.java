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

package uk.gov.gchq.gaffer.operation;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;

/**
 * A <code>GetOperation</code> defines a seeded get operation to be processed on a graph.
 * <p>
 * Get operations have several flags to determine how to filter {@link uk.gov.gchq.gaffer.data.element.Element}s.
 *
 * @param <SEED_TYPE>   the seed type of the operation. This must be JSON serialisable.
 * @param <RETURN_TYPE> the result type of the operation. This must be JSON serialisable.
 */
public interface GetOperation<SEED_TYPE, RETURN_TYPE>
        extends Operation<CloseableIterable<SEED_TYPE>, RETURN_TYPE> {

    /**
     * A <code>IncludeEdgeType</code> defines whether {@link uk.gov.gchq.gaffer.data.element.Edge}s used in the operation should
     * be directed.
     */
    enum IncludeEdgeType {
        ALL, DIRECTED, UNDIRECTED, NONE;

        boolean accept(final boolean directed) {
            return ALL.equals(this) || (DIRECTED.equals(this) && directed) || (UNDIRECTED.equals(this) && !directed);
        }
    }

    /**
     * A <code>IncludeIncomingOutgoingType</code> defines the direction of the {@link uk.gov.gchq.gaffer.data.element.Edge}s during
     * the operation.
     */
    enum IncludeIncomingOutgoingType {
        BOTH, INCOMING, OUTGOING
    }

    /**
     * A <code>SeedMatchingType</code> defines how the seeds in the operation should be matched.
     */
    enum SeedMatchingType {
        EQUAL, RELATED
    }

    /**
     * @return the {@link CloseableIterable} of input seeds (SEED_TYPE) for the operation.
     */
    CloseableIterable<SEED_TYPE> getSeeds();

    /**
     * @param seeds the {@link CloseableIterable} of input seeds (SEED_TYPE) for the operation.
     */
    void setSeeds(final CloseableIterable<SEED_TYPE> seeds);
}

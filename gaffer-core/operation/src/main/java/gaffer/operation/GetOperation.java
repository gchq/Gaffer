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

package gaffer.operation;

import gaffer.data.element.Edge;
import gaffer.data.element.Entity;

/**
 * A <code>GetOperation</code> defines a seeded get operation to be processed on a graph.
 * <p>
 * Get operations have several flags to determine how to filter {@link gaffer.data.element.Element}s.
 *
 * @param <SEED_TYPE>   the seed type of the operation. This must be JSON serialisable.
 * @param <RETURN_TYPE> the result type of the operation. This must be JSON serialisable.
 */
public interface GetOperation<SEED_TYPE, RETURN_TYPE>
        extends Operation<Iterable<SEED_TYPE>, Iterable<RETURN_TYPE>> {

    /**
     * A <code>IncludeEdgeType</code> defines whether {@link gaffer.data.element.Edge}s used in the operation should
     * be directed.
     */
    enum IncludeEdgeType {
        ALL, DIRECTED, UNDIRECTED, NONE;

        boolean accept(final boolean directed) {
            return ALL.equals(this) || (DIRECTED.equals(this) && directed) || (UNDIRECTED.equals(this) && !directed);
        }
    }

    /**
     * A <code>IncludeIncomingOutgoingType</code> defines the direction of the {@link gaffer.data.element.Edge}s during
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
     * @return true if properties should definitely populated. false if properties do not have to be populated.
     */
    boolean isPopulateProperties();

    /**
     * @param populateProperties true if properties should definitely populated. false if properties do not have to be populated.
     */
    void setPopulateProperties(final boolean populateProperties);

    /**
     * @return a {@link gaffer.operation.GetOperation.SeedMatchingType} describing how the seeds should be
     * matched to the identifiers in the graph.
     * @see gaffer.operation.GetOperation.SeedMatchingType
     */
    SeedMatchingType getSeedMatching();

    /**
     * @return the {@link java.lang.Iterable} of input seeds (SEED_TYPE) for the operation.
     */
    Iterable<SEED_TYPE> getSeeds();

    /**
     * @param seeds the {@link java.lang.Iterable} of input seeds (SEED_TYPE) for the operation.
     */
    void setSeeds(final Iterable<SEED_TYPE> seeds);

    /**
     * @param entity the entity to validate.
     * @return true if the entity is passes validation for the operation flags - e.g isIncludeEntities
     */
    boolean validateFlags(final Entity entity);

    /**
     * @param edge the edge to validate.
     * @return true if the edge is passes validation for the operation flags - e.g getIncludeEdges
     */
    boolean validateFlags(final Edge edge);

    /**
     * @return true if {@link gaffer.data.element.Entity}s should be included, otherwise false.
     */
    boolean isIncludeEntities();

    /**
     * The result set should include {@link gaffer.data.element.Entity}s.
     *
     * @param includeEntities set to TRUE to return {@link gaffer.data.element.Entity}s
     */
    void setIncludeEntities(final boolean includeEntities);

    /**
     * @return includeIncomingOutGoing a {@link gaffer.operation.GetOperation.IncludeIncomingOutgoingType}
     * that controls the direction of {@link gaffer.data.element.Edge}s that are
     * filtered out in the operation.
     * @see gaffer.operation.GetOperation.IncludeIncomingOutgoingType
     */
    IncludeIncomingOutgoingType getIncludeIncomingOutGoing();

    /**
     * @param includeIncomingOutGoing a {@link gaffer.operation.GetOperation.IncludeIncomingOutgoingType}
     *                                that controls the direction of {@link gaffer.data.element.Edge}s that are
     *                                filtered out in the operation.
     * @see gaffer.operation.GetOperation.IncludeIncomingOutgoingType
     */
    void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType includeIncomingOutGoing);

    /**
     * @param includeEdges a {@link gaffer.operation.GetOperation.IncludeEdgeType} controls which
     *                     {@link gaffer.data.element.Edge}s are filtered out in the operation.
     * @see gaffer.operation.GetOperation.IncludeEdgeType
     */
    void setIncludeEdges(final IncludeEdgeType includeEdges);

    /**
     * @return includeEdges an {@link IncludeEdgeType} that controls which {@link gaffer.data.element.Edge}s
     * are filtered out in the operation.
     * @see gaffer.operation.GetOperation.IncludeEdgeType
     */
    IncludeEdgeType getIncludeEdges();

    /**
     * @return true if the graph should aggregate results where partial key matches are found,
     * e.g. summarised over time. Otherwise false.
     */
    boolean isSummarise();

    /**
     * If true then the graph should aggregate results where partial key matches are found.
     * e.g. summarised over time
     *
     * @param summarise set to true to summarise results
     */
    void setSummarise(final boolean summarise);
}

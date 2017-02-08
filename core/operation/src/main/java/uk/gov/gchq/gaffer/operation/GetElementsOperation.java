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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;

/**
 * A <code>GetElementsOperation</code> defines a seeded get operation to be processed on a graph.
 * <p>
 * GetElements operations have several flags to determine how to filter {@link uk.gov.gchq.gaffer.data.element.Element}s.
 *
 * @param <SEED_TYPE>   the seed type of the operation. This must be JSON serialisable.
 * @param <RETURN_TYPE> the result type of the operation. This must be JSON serialisable.
 */
public interface GetElementsOperation<SEED_TYPE, RETURN_TYPE> extends GetOperation<SEED_TYPE, RETURN_TYPE> {

    /**
     * @return true if properties should definitely populated. false if properties do not have to be populated.
     */
    boolean isPopulateProperties();

    /**
     * @param populateProperties true if properties should definitely populated. false if properties do not have to be populated.
     */
    void setPopulateProperties(final boolean populateProperties);

    /**
     * @return a {@link SeedMatchingType} describing how the seeds should be
     * matched to the identifiers in the graph.
     * @see SeedMatchingType
     */
    SeedMatchingType getSeedMatching();

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
     * @return true if {@link Entity}s should be included, otherwise false.
     */
    boolean isIncludeEntities();

    /**
     * The result set should include {@link Entity}s.
     *
     * @param includeEntities set to TRUE to return {@link Entity}s
     */
    void setIncludeEntities(final boolean includeEntities);

    /**
     * @return includeIncomingOutGoing a {@link IncludeIncomingOutgoingType}
     * that controls the direction of {@link Edge}s that are
     * filtered out in the operation.
     * @see IncludeIncomingOutgoingType
     */
    IncludeIncomingOutgoingType getIncludeIncomingOutGoing();

    /**
     * @param includeIncomingOutGoing a {@link IncludeIncomingOutgoingType}
     *                                that controls the direction of {@link Edge}s that are
     *                                filtered out in the operation.
     * @see IncludeIncomingOutgoingType
     */
    void setIncludeIncomingOutGoing(final IncludeIncomingOutgoingType includeIncomingOutGoing);

    /**
     * @param includeEdges a {@link IncludeEdgeType} controls which
     *                     {@link Edge}s are filtered out in the operation.
     * @see IncludeEdgeType
     */
    void setIncludeEdges(final IncludeEdgeType includeEdges);

    /**
     * @return includeEdges an {@link IncludeEdgeType} that controls which {@link Edge}s
     * are filtered out in the operation.
     * @see IncludeEdgeType
     */
    IncludeEdgeType getIncludeEdges();
}

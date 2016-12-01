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

package uk.gov.gchq.gaffer.accumulostore.key;

import org.apache.accumulo.core.data.Range;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import java.util.List;

/**
 * The range factory is designed so that a List of Accumulo
 * {@link org.apache.accumulo.core.data.Range}s can be created from just a
 * provided {@link uk.gov.gchq.gaffer.operation.data.ElementSeed} and
 * {@link uk.gov.gchq.gaffer.operation.GetOperation} The created range list should contain
 * all the ranges (preferably optimised) necessary to retrieve all desired
 * {@link uk.gov.gchq.gaffer.data.element.Element}s as expressed by the
 * gaffer.accumulostore.operation.
 */
public interface RangeFactory {

    /**
     * Returns a Range representing a query for the given ID
     *
     * @param elementSeed the element seed to get the range for
     * @param operation   the operation
     * @param <T>         the type of operation
     * @return A List of Ranges that are required to return all elements that
     * match the parameters of the query.
     * @throws RangeFactoryException if a range could not be created
     */
    <T extends GetElementsOperation<?, ?>> List<Range> getRange(final ElementSeed elementSeed, final T operation)
            throws RangeFactoryException;

    /**
     * Operation Returns a Range representing a query for all values between the
     * given {@link uk.gov.gchq.gaffer.operation.data.ElementSeed}s taken from the minimum
     * comparable byte value of the provided keys and the maximum comparable
     * byte value. Note that depending on the serialisation mechanism used and
     * your key design the results of a range query will differ. The intent here
     * is that values in your gaffer.accumulostore instance should be ordered
     * within the table so that Entities with an Integer Identifier occur in the
     * order 1 2 3
     * <p>
     * So that a provided pair of 1 and 3 will return entities 1, 2 and 3.
     *
     * @param pairRange the pair of element seeds to get the range for
     * @param operation the operation
     * @param <T>       the type of operation
     * @return A List of Ranges that are required to return all elements that
     * match the parameters of the query.
     * @throws RangeFactoryException if a range could not be created
     */
    <T extends GetElementsOperation<?, ?>> Range getRangeFromPair(final Pair<ElementSeed> pairRange, final T operation)
            throws RangeFactoryException;
}

/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.predicate.time;

import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.BiPredicate;
import gaffer.predicate.Predicate;

import java.util.Date;

/**
 * A predicate to indicate whether a pair of {@link Date}s, that correspond to the start and end of
 * the summary period of a {@link GraphElement}, are wanted.
 *
 * A concrete instance of this abstract class is automatically a {@link Predicate} of {@link GraphElementWithStatistics} -
 * the predicate is applied to the start and end date of the {@link GraphElementWithStatistics}.
 */
public abstract class TimePredicate implements BiPredicate<Date, Date>, Predicate<GraphElementWithStatistics> {

    private static final long serialVersionUID = -1709191991229917034L;

    @Override
    public abstract boolean accept(Date startDate, Date endDate);

    @Override
    public boolean accept(GraphElementWithStatistics graphElementWithStatistics) {
        return accept(graphElementWithStatistics.getStartDate(), graphElementWithStatistics.getEndDate());
    }

}

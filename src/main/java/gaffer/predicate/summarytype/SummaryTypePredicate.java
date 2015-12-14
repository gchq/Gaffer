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
package gaffer.predicate.summarytype;

import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.BiPredicate;

import gaffer.predicate.Predicate;

/**
 * Interface for predicates to determine if a summary type and subtype
 * are acceptable.
 */
public abstract class SummaryTypePredicate implements BiPredicate<String, String>, Predicate<GraphElementWithStatistics> {

	private static final long serialVersionUID = -5391888143714387330L;

	@Override
	public abstract boolean accept(String summaryType, String summarySubType);

	@Override
	public boolean accept(GraphElementWithStatistics graphElementWithStatistics) {
		return accept(graphElementWithStatistics.getSummaryType(), graphElementWithStatistics.getSummarySubType());
	}

}

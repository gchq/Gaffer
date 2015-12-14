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
package gaffer.accumulo.iterators;

import gaffer.Pair;
import gaffer.accumulo.Constants;
import gaffer.accumulo.ConversionUtils;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;

/**
 * Combines Edges and SetOfStatistics over different time windows and visibilities.
 * e.g. if we have an edge:
 * 
 * srcType = X, srcValue = A, dstType = Y, dstValue = B,
 * 		type = XXX, subType = YYY, directed = true, visibility = public, start = 20140101, end = 20140102
 * 
 * and an edge:
 * 
 * srcType = X, srcValue = A, dstType = Y, dstValue = B,
 * 		type = XXX, subType = YYY, directed = true, visibility = private, start = 20140102, end = 20140103
 * 
 * then this will be rolled up into the following edge
 * 
 * srcType = X, srcValue = A, dstType = Y, dstValue = B,
 * 		type = XXX, subType = YYY, directed = true, visibility = public/private, start = 20140101, end = 20140103
 * 
 * The corresponding {@link SetOfStatistics} from the values will also be merged.
 */
public class RollUpOverTimeAndVisibility extends CombinerOverColumnQualifierAndVisibility {

	@Override
	public ColumnQualifierColumnVisibilityValueTriple reduce(Key key,
			Iterator<ColumnQualifierColumnVisibilityValueTriple> iter) {
		
		Date startDate = null;
		Date endDate = null;
		Set<String> visibilities = new TreeSet<String>(); // Use a TreeSet to ensure consistent ordering of visibilities
		SetOfStatistics setOfStatistics = new SetOfStatistics();
		int numberOfExceptions = 0;
		
		while (iter.hasNext()) {
			
			ColumnQualifierColumnVisibilityValueTriple cqcvvt = iter.next();
			try {
				// Get dates from column qualifier.
				// Keep a record of the earliest start date and the latest end date we find.
				Pair<Date> pair = ConversionUtils.getDatesFromColumnQualifier(cqcvvt.getColumnQualifier());
				
				if (startDate == null) {
					startDate = pair.getFirst();
				} else if (pair.getFirst().before(startDate)) {
					startDate = pair.getFirst();
				}
				if (endDate == null) {
					endDate = pair.getSecond();
				} else if (pair.getSecond().after(endDate)) {
					endDate = pair.getSecond();
				}
				// Add visibility to set of visibilities.
				visibilities.add(cqcvvt.getColumnVisibility());
				// Merge statistics
				setOfStatistics.merge(ConversionUtils.getSetOfStatisticsFromValue(cqcvvt.getValue()));
			} catch (IOException e) {
				numberOfExceptions++;
			}
			
		}
		
		if (numberOfExceptions != 0) {
			setOfStatistics.addStatistic(Constants.ERROR_IN_ROLL_UP_OVER_TIME_AND_VISIBILITY_ITERATOR, new Count(numberOfExceptions));
		}
		
		String resultColumnVisibility = "";
		for (String s : visibilities) {
			if (!resultColumnVisibility.equals("")) {
				resultColumnVisibility += "&" + s;
			} else {
				resultColumnVisibility += s;
			}
		}
		
		ColumnQualifierColumnVisibilityValueTriple result = new ColumnQualifierColumnVisibilityValueTriple(
				ConversionUtils.getColumnQualifierFromDates(startDate, endDate),
				resultColumnVisibility,
				ConversionUtils.getValueFromSetOfStatistics(setOfStatistics));
		
		return result;
	}

}

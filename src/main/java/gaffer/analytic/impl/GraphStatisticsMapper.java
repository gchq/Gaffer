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
package gaffer.analytic.impl;

import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.FirstSeen;
import gaffer.statistics.impl.LastSeen;
import gaffer.statistics.impl.LongCount;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Takes in ({@link GraphElement}, {@link SetOfStatistics}) pairs and outputs ({@link Text},
 * {@link SetOfStatistics}) pairs, where the text is the:
 * 	
 *  - Entity type
 *  - Entity summary type
 *  - Entity summary subtype
 *  - Entity summary type and subtype
 *  - Entity visibility
 *  - Edge source type
 *  - Edge destination type
 *  - Edge summary type
 *  - Edge summary subtype
 *  - Edge visibility
 *  
 * and the statistics contain:
 * 
 *  - Count (e.g. number of entities with that type)
 *  - First seen (the earliest start date of that type)
 *  - Last seen (the latest start date of that type)
 */
public class GraphStatisticsMapper extends Mapper<GraphElement, SetOfStatistics, Text, SetOfStatistics> {

	public final static String COUNTER_GROUP_NAME = "Gaffer";
	public final static String TOTAL_ELEMENTS = "Total elements";
	public final static String TOTAL_ENTITIES = "Total entities";
	public final static String TOTAL_EDGES = "Total edges";

	private Map<Text, SetOfStatistics> cache;
	private final static int MAX_CACHE_SIZE = 10000;

	protected void setup(Context context) throws IOException, InterruptedException {
		cache = new HashMap<Text, SetOfStatistics>();
	}

	protected void map(GraphElement element, SetOfStatistics statistics, Context context) throws IOException, InterruptedException {
		context.getCounter(COUNTER_GROUP_NAME, TOTAL_ELEMENTS).increment(1L);
		if (element.isEntity()) {
			outputInformationFromEntity(element.getEntity(), context);
			context.getCounter(COUNTER_GROUP_NAME, TOTAL_ENTITIES).increment(1L);
		} else {
			outputInformationFromEdge(element.getEdge(), context);
			context.getCounter(COUNTER_GROUP_NAME, TOTAL_EDGES).increment(1L);
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		flushCache(context);
	}

	private void outputInformationFromEntity(Entity entity, Context context) throws IOException, InterruptedException {
		SetOfStatistics statistics = getStatistics(entity.getStartDate(), entity.getEndDate());
		addToCache(new Text("EntityType=" + entity.getEntityType()), statistics, context);
		addToCache(new Text("EntitySummaryType=" + entity.getSummaryType()), statistics, context);
		addToCache(new Text("EntitySummarySubType=" + entity.getSummarySubType()), statistics, context);
		addToCache(new Text("EntitySummaryTypeAndSubType=" + entity.getSummaryType() + "," + entity.getSummarySubType()),
				statistics, context);
		addToCache(new Text("Visibility=" + entity.getVisibility()), statistics, context);
	}

	private void outputInformationFromEdge(Edge edge, Context context) throws IOException, InterruptedException {
		SetOfStatistics statistics = getStatistics(edge.getStartDate(), edge.getEndDate());
		addToCache(new Text("EdgeSourceType=" + edge.getSourceType()), statistics, context);
		addToCache(new Text("EdgeDestinationType=" + edge.getDestinationType()), statistics, context);
		addToCache(new Text("EdgeSummaryType=" + edge.getSummaryType()), statistics, context);
		addToCache(new Text("EdgeSummarySubType=" + edge.getSummarySubType()), statistics, context);
		addToCache(new Text("EdgeSummaryTypeAndSubType=" + edge.getSummaryType() + "," + edge.getSummarySubType()),
				statistics, context);
		addToCache(new Text("Visibility=" + edge.getVisibility()), statistics, context);
	}

	private void addToCache(Text text, SetOfStatistics stats, Context context) throws IOException, InterruptedException {
		if (cache.containsKey(text)) {
			cache.get(text).merge(stats);
		} else {
			cache.put(text, stats);
		}
		if (cache.size() > MAX_CACHE_SIZE) {
			flushCache(context);
		}
	}

	private void flushCache(Context context) throws IOException, InterruptedException {
		for (Text t : cache.keySet()) {
			context.write(t, cache.get(t));
		}
		cache.clear();
	}

	private static SetOfStatistics getStatistics(Date start, Date end) {
		SetOfStatistics statistics = new SetOfStatistics();
		statistics.addStatistic("count", new LongCount(1L));
		statistics.addStatistic("earliestElementStartDate", new FirstSeen(start));
		statistics.addStatistic("earliestElementEndDate", new LastSeen(end));
		return statistics;
	}

}

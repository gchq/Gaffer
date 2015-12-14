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
package gaffer.accumulo;

import gaffer.Pair;
import gaffer.accumulo.utils.StringEscapeUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.TypeValue;
import gaffer.graph.TypeValueRange;
import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.WritableUtils;

/**
 * Utility functions to convert between graph objects ({@link Entity}s and {@link Edge}s)
 * and Accumulo {@link Key}s and {@link Value}s.
 */
public class ConversionUtils {

	private ConversionUtils() {}

	// Methods to get Accumulo keys from graph elements
	/**
	 * Converts a {@link GraphElement} to one or two Accumulo {@link Key}s.
	 * 
	 * @param element
	 * @return
	 */
	public static Pair<Key> getKeysFromGraphElement(GraphElement element) {
		if (element.isEntity()) {
			Key key = getKeyFromEntity(element.getEntity());
			Pair<Key> pair = new Pair<Key>(key, null);
			return pair;
		} else {
			Pair<Key> pair = getKeysFromEdge(element.getEdge());
			return pair;
		}
	}

	/**
	 * Converts an {@link Edge} to a pair of {@link Key}s.
	 * 
	 * @param edge
	 * @return
	 */
	public static Pair<Key> getKeysFromEdge(Edge edge) {
		// Get pair of row keys
		Pair<String> rowKeys = getRowKeysFromEdge(edge);

		// Column family is edge type and subtype
		String columnFamily = getColumnFamilyFromEdge(edge);

		// Column qualifier is start and end dates (written as longs)
		String columnQualifier = getColumnQualifier(edge);

		// Column visibility
		ColumnVisibility columnVisibility = new ColumnVisibility(edge.getVisibility());

		// Timestamp - equal to the time of the end date of the edge
		long timestamp = edge.getEndDate().getTime();

		// Create Accumulo keys - note that second row key may be null (if it's a self-edge) and
		// in that case we should return null second key
		Key key1 = new Key(rowKeys.getFirst(), columnFamily, columnQualifier, columnVisibility, timestamp);
		Key key2 = rowKeys.getSecond() != null ? new Key(rowKeys.getSecond(), columnFamily, columnQualifier, columnVisibility, timestamp) : null;

		// Return pair of keys
		Pair<Key> result = new Pair<Key>(key1, key2);
		return result;
	}

	/**
	 * Converts an {@link Entity} to a {@link Key}.
	 * 
	 * @param entity
	 * @return
	 */
	public static Key getKeyFromEntity(Entity entity) {
		// Row key is formed from type and value
		String rowKey = getRowKeyFromEntity(entity);

		// Column family is summary type and summary subtype
		String columnFamily = getColumnFamilyFromEntity(entity);

		// Column qualifier is start and end dates (written as longs)
		String columnQualifier = getColumnQualifierFromEntity(entity);

		// Column visibility is formed from the visibility
		String columnVisibility = entity.getVisibility();

		// Timestamp - equal to the time of the end date of the Entity
		long timestamp = entity.getEndDate().getTime();

		// Create and return key
		Key key = new Key(rowKey, columnFamily, columnQualifier, columnVisibility, timestamp);
		return key;
	}

	// Methods to get graph elements from Accumulo keys

	/**
	 * Gets a GraphElement from a key
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public static GraphElement getGraphElementFromKey(Key key) throws IOException {
		boolean keyRepresentsEntity = doesKeyRepresentEntity(key.getRow().toString());
		if (keyRepresentsEntity) {
			return new GraphElement(getEntityFromKey(key));
		}
		return new GraphElement(getEdgeFromKey(key));
	}

	/**
	 * Gets an edge from a key - this assumes that the key does indeed represent
	 * an edge.
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public static Edge getEdgeFromKey(Key key) throws IOException {
		// Get sourceType, sourceValue, destinationType, destinationValue and direction from row key
		String[] result = new String[4];
		boolean directed = getSourceAndDestinationFromRowKey(key.getRow().toString(), result);
		String sourceType = result[0];
		String sourceValue = result[1];
		String destinationType = result[2];
		String destinationValue = result[3];

		// Get edge type and subtype from column family
		Pair<String> pair1 = getTypeAndSubtypeFromColumnFamily(key.getColumnFamily().toString());
		String edgeType = pair1.getFirst();
		String edgeSubType = pair1.getSecond();

		// Get start and end dates from column qualifier
		Pair<Date> pair2 = getDatesFromColumnQualifier(key.getColumnQualifier().toString());
		Date startDate = pair2.getFirst();
		Date endDate = pair2.getSecond();

		// Get visibility from column visibility.
		String visibility = key.getColumnVisibility().toString();

		// Create and return edge
		Edge e = new Edge(sourceType, sourceValue, destinationType, destinationValue, edgeType, edgeSubType, directed, visibility, startDate, endDate);
		return e;
	}

	/**
	 * Gets an entity from a key - this assumes that the key does indeed represent
	 * an entity.
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public static Entity getEntityFromKey(Key key) throws IOException {
		// Get entity type and entity value from row key
		Pair<String> pair1 = getEntityTypeAndValueFromKey(key.getRow().toString());
		String entityType = pair1.getFirst();
		String entityValue = pair1.getSecond();

		// Get summary type and subtype from column family
		Pair<String> pair2 = getTypeAndSubtypeFromColumnFamily(key.getColumnFamily().toString());
		String summaryType = pair2.getFirst();
		String summarySubtype = pair2.getSecond();

		// Get start and end dates from column qualifier
		Pair<Date> pair3 = getDatesFromColumnQualifier(key.getColumnQualifier().toString());
		Date startDate = pair3.getFirst();
		Date endDate = pair3.getSecond();

		// Get visibility from column visibility
		String visibility = key.getColumnVisibility().toString();

		// Create and return entity
		return new Entity(entityType, entityValue, summaryType, summarySubtype, visibility, startDate, endDate);
	}

	// Methods to move from SetOfStatistics to Value and back.
	/**
	 * Gets a {@link Value} from a {@link SetOfStatistics} by using the
	 * its {@link SetOfStatistics#write} method.
	 * 
	 * @param statistics
	 * @return
	 */
	public static Value getValueFromSetOfStatistics(SetOfStatistics statistics) {
		return new Value(WritableUtils.toByteArray(statistics));
	}

	/**
	 * Deserialises a {@link Value} into a {@link SetOfStatistics} by using
	 * the read method applied to the byte array from the {@link Value}.
	 * 
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static SetOfStatistics getSetOfStatisticsFromValue(Value value) throws IOException {
		DataInput in = new DataInputStream(new ByteArrayInputStream(value.get()));
		SetOfStatistics statistics = new SetOfStatistics();
		statistics.readFields(in);
		return statistics;
	}


	// Simple utility methods for entities
	/**
	 * Determines whether the given string corresponds to a row key representing an
	 * {@link Entity}. If the row key does represent an Entity then true is
	 * returned; if it represents an Edge then false is returned; if it represents
	 * neither then an {@link IOException} is thrown.
	 * 
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public static boolean doesKeyRepresentEntity(String rowKey) throws IOException {
		char[] rowKeyCharArray = rowKey.toCharArray();
		short numDelims = 0;
		for (int i = 0; i < rowKeyCharArray.length; i++) {
			if (rowKeyCharArray[i] == Constants.DELIMITER) {
				numDelims++;
			}
		}
		if (numDelims != 1 && numDelims != 4) {
			throw new IOException("Wrong number of delimiters found in row key - found " + numDelims + ", expected 1 or 4.");
		}
		if (numDelims == 1) {
			// Must be entity
			return true;
		}
		// Must be edge
		return false;
	}

	public static String getRowKeyFromTypeAndValue(String type, String value) {
		return StringEscapeUtils.escape(type) + Constants.DELIMITER + StringEscapeUtils.escape(value);
	}

	public static String getRowKeyFromEntity(Entity entity) {
		return StringEscapeUtils.escape(entity.getEntityType()) + Constants.DELIMITER + StringEscapeUtils.escape(entity.getEntityValue());
	}

	public static String getColumnFamilyFromEntity(Entity entity) {
		return StringEscapeUtils.escape(entity.getSummaryType()) + Constants.DELIMITER + StringEscapeUtils.escape(entity.getSummarySubType());
	}

	private static String getColumnQualifierFromEntity(Entity entity) {
		return getColumnQualifierFromDates(entity.getStartDate(), entity.getEndDate());
	}

	public static Pair<String> getEntityTypeAndValueFromKey(String rowKey) throws IOException {
		char[] rowKeyCharArray = rowKey.toCharArray();
		int positionOfDelimiter = -1;
		short numDelims = 0;
		for (int i = 0; i < rowKeyCharArray.length; i++) {
			if (rowKeyCharArray[i] == Constants.DELIMITER) {
				positionOfDelimiter = i;
				numDelims++;
				if (numDelims > 1) {
					throw new IOException("Too many delimiters found in row key - found more than the expected 1.");
				}
			}
		}
		if (numDelims != 1) {
			throw new IOException("Wrong number of delimiters found in row key - found " + numDelims + ", expected 1.");
		}
		String entityType = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, 0, positionOfDelimiter)));
		String entityValue = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionOfDelimiter + 1, rowKeyCharArray.length)));
		return new Pair<String>(entityType, entityValue);
	}

	// Simple utility methods for edges
	public static Pair<String> getRowKeysFromEdge(Edge edge) {
		// If edge is undirected then direction flag is 0 for both keys.
		// If edge is directed then for the first key the direction flag is 1
		// indicating that the first type-value in the key is the source
		// and the second is the destination, and for the second key the
		// direction flag is 2 indicating that the first type-value in the
		// key is the destination type-value and the second type-value in
		// the key is the source type-value, i.e. they need flipping around.
		int directionFlag1 = (edge.isDirected() ? 1 : 0);
		int directionFlag2 = (edge.isDirected() ? 2 : 0);
		String rowKey1 = StringEscapeUtils.escape(edge.getSourceType())
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(edge.getSourceValue())
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(edge.getDestinationType())
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(edge.getDestinationValue())
				+ Constants.DELIMITER
				+ directionFlag1;
		String rowKey2 = StringEscapeUtils.escape(edge.getDestinationType())
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(edge.getDestinationValue())
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(edge.getSourceType())
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(edge.getSourceValue())
				+ Constants.DELIMITER
				+ directionFlag2;
		// Is this a self-edge? If so then return null for the second rowkey as
		// we don't want the same edge to go into Accumulo twice.
		if (selfEdge(edge)) {
			return new Pair<String>(rowKey1, null);
		}
		return new Pair<String>(rowKey1, rowKey2);
	}

	/**
	 * Returns true is this is a self-edge, i.e. the source type equals the
	 * destination type, and the source value equals the destination value.
	 * 
	 * @param edge
	 * @return
	 */
	private static boolean selfEdge(Edge edge) {
		return edge.getSourceType().equals(edge.getDestinationType()) && edge.getSourceValue().equals(edge.getDestinationValue());
	}

	public static String getColumnFamilyFromEdge(Edge edge) {
		String columnFamily = StringEscapeUtils.escape(edge.getSummaryType())
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(edge.getSummarySubType());
		return columnFamily;
	}

	public static String getColumnQualifier(Edge edge) {
		return getColumnQualifierFromDates(edge.getStartDate(), edge.getEndDate());
	}

	public static boolean getSourceAndDestinationFromRowKey(String rowKey, String[] sourceTypeValueDestinationTypeValue) throws IOException {
		// Get sourceType, sourceValue, destinationType, destinationValue and directed flag from row key
		char[] rowKeyCharArray = rowKey.toCharArray();
		int[] positionsOfDelimiters = new int[4]; // Expect to find 4 delimiters (5 fields)
		short numDelims = 0;
		for (int i = 0; i < rowKeyCharArray.length; i++) {
			if (rowKeyCharArray[i] == Constants.DELIMITER) {
				if (numDelims >= 4) {
					throw new IOException("Too many delimiters found in row key - found more than the expected 4.");
				}
				positionsOfDelimiters[numDelims++] = i;
			}
		}
		if (numDelims != 4) {
			throw new IOException("Wrong number of delimiters found in row key - found " + numDelims + ", expected 4.");
		}
		// If edge is undirected then create edge (no need to worry about which direction the type-values
		// should go in).
		// If the edge is directed then need to decide which way round the type-values should go.
		int directionFlag = -1;
		try {
			directionFlag = Integer.parseInt(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[3] + 1, rowKeyCharArray.length)));
		} catch (NumberFormatException e) {
			throw new IOException("Error parsing direction flag from row key - " + e);
		}
		if (directionFlag == 0) {
			// Edge is undirected
			sourceTypeValueDestinationTypeValue[0] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, 0, positionsOfDelimiters[0])));
			sourceTypeValueDestinationTypeValue[1] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[0] + 1, positionsOfDelimiters[1])));
			sourceTypeValueDestinationTypeValue[2] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[1] + 1, positionsOfDelimiters[2])));
			sourceTypeValueDestinationTypeValue[3] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[2] + 1, positionsOfDelimiters[3])));
			return false;
		} else if (directionFlag == 1) {
			// Edge is directed and the first type-value is the source of the edge
			sourceTypeValueDestinationTypeValue[0] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, 0, positionsOfDelimiters[0])));
			sourceTypeValueDestinationTypeValue[1] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[0] + 1, positionsOfDelimiters[1])));
			sourceTypeValueDestinationTypeValue[2] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[1] + 1, positionsOfDelimiters[2])));
			sourceTypeValueDestinationTypeValue[3] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[2] + 1, positionsOfDelimiters[3])));
			return true;
		} else if (directionFlag == 2) {
			// Edge is directed and the second type-value is the source of the edge
			sourceTypeValueDestinationTypeValue[0] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[1] + 1, positionsOfDelimiters[2])));
			sourceTypeValueDestinationTypeValue[1] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[2] + 1, positionsOfDelimiters[3])));
			sourceTypeValueDestinationTypeValue[2] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, 0, positionsOfDelimiters[0])));
			sourceTypeValueDestinationTypeValue[3] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[0] + 1, positionsOfDelimiters[1])));
			return true;
		} else {
			throw new IOException("Invalid direction flag in row key - flag was " + directionFlag);
		}
	}

	public static String[] getFirstAndSecondTypeValuesFromRowKey(String rowKey) throws IOException {
		char[] rowKeyCharArray = rowKey.toCharArray();
		int[] positionsOfDelimiters = new int[4]; // Expect to find 4 delimiters (5 fields)
		short numDelims = 0;
		for (int i = 0; i < rowKeyCharArray.length; i++) {
			if (rowKeyCharArray[i] == Constants.DELIMITER) {
				if (numDelims >= 4) {
					throw new IOException("Too many delimiters found in row key - found more than the expected 4.");
				}
				positionsOfDelimiters[numDelims++] = i;
			}
		}
		if (numDelims != 4) {
			throw new IOException("Wrong number of delimiters found in row key - found " + numDelims + ", expected 4.");
		}
		String[] firstTypeValueSecondTypeValue = new String[4];
		firstTypeValueSecondTypeValue[0] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, 0, positionsOfDelimiters[0])));
		firstTypeValueSecondTypeValue[1] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[0] + 1, positionsOfDelimiters[1])));
		firstTypeValueSecondTypeValue[2] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[1] + 1, positionsOfDelimiters[2])));
		firstTypeValueSecondTypeValue[3] = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(rowKeyCharArray, positionsOfDelimiters[2] + 1, positionsOfDelimiters[3])));
		return firstTypeValueSecondTypeValue;
	}

	private static int getNumTrailingDelimPlusOne(char[] charArray) {
		int num = 0;
		for (int i = charArray.length - 1; i >= 0; i--) {
			if (charArray[i] == Constants.DELIMITER_PLUS_ONE) {
				num++;
			} else {
				break;
			}
		}
		return num;
	}

	/**
	 * Returns the first type-value in the row key as a byte array, i.e. the type and value
	 * converted to a byte array. Note this is independent of the direction of the edge, i.e.
	 * if the key represents an edge where the second type-value in the Accumulo row key
	 * is the source then the first type-value is returned. This method is used to convert
	 * Accumulo keys into Bloom filter keys.
	 * 
	 * If the key represents neither an {@link Entity}, nor an {@link Edge} nor
	 * a key formed to query for a range corresponding to a {@link TypeValue}, then
	 * an IOException is thrown.
	 * 
	 * Note that this is quite complex. When we query for all data involving a {@link TypeValue},
	 * we create a range starting at the TypeValue and ending at the TypeValue with
	 * Constants.DELIMITER_PLUS_ONE added. This ensures that we get all edges involving
	 * the TypeValue. However, in order to take advantage of the Bloom filters we need
	 * to map this range to one type-value, and this requires mapping both the start
	 * and end of the range to the same key (only in the case where the range corresponds
	 * to one TypeValue of course). So if there is a trailing Constants.DELIMITER_PLUS_ONE
	 * we need to remove it. But we need to check that this is not part of the actual value.
	 * We can do this because Constants.DELIMITER_PLUS_ONE is used as an escape character,
	 * and so there will be two of them if it's actually part of the data.
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	public static byte[] getFirstTypeValue(Key key) throws IOException {
		// Read through until either reach second Constants.DELIMITER or end of row key.
		// In former case return bytes up to (but not including) Constants.DELIMITER; in
		// latter case return entire row key.
		char[] rowKeyCharArray = key.getRow().toString().toCharArray();
		short numDelims = 0;
		int positionOfSecondDelimiter = -1;
		for (int i = 0; i < rowKeyCharArray.length; i++) {
			if (rowKeyCharArray[i] == Constants.DELIMITER) {
				numDelims++;
				if (numDelims == 2) {
					positionOfSecondDelimiter = i;
					break;
				}
			}
		}
		// Need to check that this key can be converted to an type-value.
		if (numDelims == 0) {
			throw new IOException("Exception obtaining first type-value from key " + key);
		}
		if (positionOfSecondDelimiter == -1) {
			// Need to see if the last character is Constants.DELIMITER_PLUS_ONE, and if it is
			// decide whether that was added when forming a range to get all data for a
			// TypeValue. We can tell this by seeing whether there are is an even number of
			// Constants.DELIMITER_PLUS_ONE at the end. If there is an even number then they
			// are part of the type-value; if there is an odd number then the last one
			// was added when forming a range.
			if (rowKeyCharArray[rowKeyCharArray.length - 1] != Constants.DELIMITER_PLUS_ONE) {
				return key.getRowData().getBackingArray();
			}
			if (getNumTrailingDelimPlusOne(rowKeyCharArray) % 2 == 0) {
				return key.getRowData().getBackingArray();
			}
			byte[] accumuloRow = key.getRowData().getBackingArray();
			byte[] bloomKey = new byte[accumuloRow.length - 1];
			System.arraycopy(accumuloRow, 0, bloomKey, 0, accumuloRow.length - 1);
			return bloomKey;
		}
		byte[] accumuloRow = key.getRowData().getBackingArray();
		byte[] bloomKey = new byte[positionOfSecondDelimiter];
		System.arraycopy(accumuloRow, 0, bloomKey, 0, positionOfSecondDelimiter);
		return bloomKey;
	}

	public static Pair<String> getTypeAndSubtypeFromColumnFamily(String columnFamily) throws IOException {
		char[] columnFamilyCharArray = columnFamily.toCharArray();
		int positionOfDelimiter = -1;
		short numDelims = 0;
		for (int i = 0; i < columnFamilyCharArray.length; i++) {
			if (columnFamilyCharArray[i] == Constants.DELIMITER) {
				positionOfDelimiter = i;
				numDelims++;
				if (numDelims > 1) {
					throw new IOException("Too many delimiters found in column family - found more than the expected 1.");
				}
			}
		}
		if (numDelims != 1) {
			throw new IOException("Wrong number of delimiters found in column family - found " + numDelims + ", expected 1.");
		}
		String type = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(columnFamilyCharArray, 0, positionOfDelimiter)));
		String subtype = StringEscapeUtils.unEscape(new String(Arrays.copyOfRange(columnFamilyCharArray, positionOfDelimiter + 1, columnFamilyCharArray.length)));
		return new Pair<String>(type, subtype);
	}

	public static Pair<Date> getDatesFromColumnQualifier(String columnQualifier) throws IOException {
		// Get start and end dates from column qualifier
		char[] columnQualifierCharArray = columnQualifier.toCharArray();
		int positionOfDelimiter = -1;
		int numDelims = 0;
		for (int i = 0; i < columnQualifierCharArray.length; i++) {
			if (columnQualifierCharArray[i] == Constants.DELIMITER) {
				positionOfDelimiter = i;
				numDelims++;
				if (numDelims > 1) {
					throw new IOException("Too many delimiters found in column qualifier - found more than the expected 1.");
				}
			}
		}
		if (numDelims != 1) {
			throw new IOException("Wrong number of delimiters found in column qualifier key - found " + numDelims + ", expected 1.");
		}
		String startDateString = new String(Arrays.copyOfRange(columnQualifierCharArray, 0, positionOfDelimiter));
		String endDateString = new String(Arrays.copyOfRange(columnQualifierCharArray, positionOfDelimiter + 1, columnQualifierCharArray.length));
		Date startDate = new Date(Long.parseLong(startDateString));
		Date endDate = new Date(Long.parseLong(endDateString));
		return new Pair<Date>(startDate, endDate);
	}

	/**
	 * Given a type and a value, returns a range that corresponds to all data involving
	 * it, i.e. both {@link Entity}s and {@link Edge}s.
	 * 
	 * @param type
	 * @param value
	 * @return
	 */
	public static Range getRangeFromTypeAndValue(String type, String value) {
		String rowKey = getRowKeyFromTypeAndValue(type, value);
		Key key = new Key(rowKey);
		Key endKey = new Key(rowKey + Constants.DELIMITER_PLUS_ONE);
		return new Range(key, true, endKey, false);
	}

	/**
	 * Given a type and a value, returns a range that will correspond to just {@link Entity}
	 * rows, i.e. no {@link Edge}s.
	 * 
	 * @param type
	 * @param value
	 * @return
	 */
	public static Range getEntityRangeFromTypeAndValue(String type, String value) {
		String rowKey = getRowKeyFromTypeAndValue(type, value);
		return Range.exact(rowKey);
	}

	/**
	 * Given a type and a value, returns a range that will correspond to just {@link Edge}
	 * rows, i.e. no {@link Entity}s.
	 * 
	 * @param type
	 * @param value
	 * @return
	 */
	public static Range getEdgeRangeFromTypeAndValue(String type, String value) {
		String rowKey = getRowKeyFromTypeAndValue(type, value);
		Key startKey = new Key(rowKey + Constants.DELIMITER); // Add delimiter to ensure that we don't get any Entitys.
		Key endKey = new Key(rowKey + Constants.DELIMITER_PLUS_ONE);
		return new Range(startKey, true, endKey, false);
	}

	/**
	 * Given a type and value, and flags to indicate whether we want to include only {@link Entity}s,
	 * only {@link Edge}s or both, it returns the appropriate range. Note that specifying neither
	 * {@link Entity}s nor {@link Edge}s will cause an {@link IllegalArgumentException}.
	 * 
	 * @param type
	 * @param value
	 * @param includeEntities
	 * @param includeEdges
	 * @return
	 */
	public static Range getRangeFromTypeAndValue(String type, String value, boolean includeEntities, boolean includeEdges) {
		if (!includeEntities && !includeEdges) {
			throw new IllegalArgumentException("Need to include either Entitys or Edges or both when getting Range from a type and value");
		}
		if (includeEntities && includeEdges) {
			return getRangeFromTypeAndValue(type, value);
		} else if (includeEntities) {
			return getEntityRangeFromTypeAndValue(type, value);
		} else {
			return getEdgeRangeFromTypeAndValue(type, value);
		}
	}

	public static Range getRangeFromPairOfTypesAndValues(String type1, String value1, String type2, String value2) {
		String rowKey = getRowKeyFromPairOfTypesAndValues(type1, value1, type2, value2);
		Key startKey = new Key(rowKey + Constants.DELIMITER);
		Key endKey = new Key(rowKey + Constants.DELIMITER_PLUS_ONE);
		return new Range(startKey, true, endKey, false);
	}

	public static String getRowKeyFromPairOfTypesAndValues(String type1, String value1, String type2, String value2) {
		String rowKey = StringEscapeUtils.escape(type1)
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(value1)
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(type2)
				+ Constants.DELIMITER
				+ StringEscapeUtils.escape(value2);
		return rowKey;
	}

	/**
	 * Converts a {@link TypeValueRange} into an Accumulo {@link Range}.
	 * The end of the range is exclusive.
	 * 
	 * @param typeValueRange
	 * @return
	 */
	public static Range getRangeFromTypeValueRange(TypeValueRange typeValueRange) {
		String startRowKey = getRowKeyFromTypeAndValue(typeValueRange.getStartType(),
				typeValueRange.getStartValue());
		Key startKey = new Key(startRowKey);
		String endRowKey = getRowKeyFromTypeAndValue(typeValueRange.getEndType(),
				typeValueRange.getEndValue());
		Key endKey = new Key(endRowKey);
		Range range = new Range(startKey, true, endKey, false);
		return range;
	}

	// General simple utility methods
	public static String getColumnQualifierFromDates(Date startDate, Date endDate) {
		return "" + startDate.getTime() + Constants.DELIMITER + endDate.getTime();
	}

}

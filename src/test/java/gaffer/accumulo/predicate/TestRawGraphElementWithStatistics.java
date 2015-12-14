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
package gaffer.accumulo.predicate;

import gaffer.Pair;
import gaffer.accumulo.ConversionUtils;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;

import static org.junit.Assert.*;

/**
 * Unit test for {@link RawGraphElementWithStatistics}. Tests that all fields are correctly
 * calculated.
 */
public class TestRawGraphElementWithStatistics {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HHmmss");
    static {
        DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
    }

    @Test
    public void testEntity() throws ParseException {
        // Create Entity and SetOfStatistics
        String type = "TYPE";
        String value = "VALUE";
        String summaryType = "SUMMARY_TYPE";
        String summarySubType = "SUMMARY_SUB_TYPE";
        String visibility = "ABC";
        Date startDate = DATE_FORMAT.parse("20131027 123456");
        Date endDate = DATE_FORMAT.parse("20131028 123456");
        Entity entity = new Entity(type, value, summaryType, summarySubType, visibility, startDate, endDate);
        SetOfStatistics statistics = new SetOfStatistics();
        statistics.addStatistic("count", new Count(10));

        // Get key and value
        Key key = ConversionUtils.getKeysFromGraphElement(new GraphElement(entity)).getFirst();
        Value accValue = ConversionUtils.getValueFromSetOfStatistics(statistics);

        // Create RawGraphElementWithStatistics
        RawGraphElementWithStatistics raw = new RawGraphElementWithStatistics(key, accValue);

        // Check that all fields are correct
        //  - Entity fields
        assertTrue(raw.isEntity());
        assertEquals(type, raw.getEntityType());
        assertEquals(value, raw.getEntityValue());
        assertEquals(new GraphElement(entity), raw.getGraphElement());
        assertEquals(summaryType, raw.getSummaryType());
        assertEquals(summarySubType, raw.getSummarySubType());
        assertEquals(visibility, raw.getVisibility());
        assertEquals(startDate, raw.getStartDate());
        assertEquals(endDate, raw.getEndDate());
        //  - Edge fields
        assertFalse(raw.isEdge());
        try {
            raw.isDirected();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        try {
            raw.getSourceType();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        try {
            raw.getSourceValue();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        try {
            raw.getDestinationType();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        try {
            raw.getDestinationValue();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        try {
            raw.getFirstType();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        try {
            raw.getFirstValue();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        try {
            raw.getSecondType();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        try {
            raw.getSecondValue();
            fail("UnsupportedOperationException should have been thrown");
        } catch (UnsupportedOperationException e) { }
        //  - Statistic fields
        assertEquals(statistics, raw.getSetOfStatistics());
    }

    @Test
    public void testEdge() throws ParseException {
        // Create Entity and SetOfStatistics
        String sourceType = "STYPE";
        String sourceValue = "SVALUE";
        String destinationType = "DTYPE";
        String destinationValue = "DVALUE";
        String summaryType = "SUMMARY_TYPE";
        String summarySubType = "SUMMARY_SUB_TYPE";
        String visibility = "ABC";
        Date startDate = DATE_FORMAT.parse("20131027 123456");
        Date endDate = DATE_FORMAT.parse("20131028 123456");
        Edge edge = new Edge(sourceType, sourceValue, destinationType, destinationValue,
                summaryType, summarySubType, true, visibility, startDate, endDate);
        SetOfStatistics statistics = new SetOfStatistics();
        statistics.addStatistic("count", new Count(10));

        Pair<Key> pair = ConversionUtils.getKeysFromGraphElement(new GraphElement(edge));

        Value accValue = ConversionUtils.getValueFromSetOfStatistics(statistics);

        int count = 0;
        for (Key key : new Key[]{pair.getFirst(), pair.getSecond()}) {
            // Create RawGraphElementWithStatistics
            RawGraphElementWithStatistics raw = new RawGraphElementWithStatistics(key, accValue);
            // Check that all fields are correct
            //  - Entity fields
            assertFalse(raw.isEntity());
            try {
                raw.getEntityType();
                fail("UnsupportedOperationException should have been thrown");
            } catch (UnsupportedOperationException e) { }
            try {
                raw.getEntityValue();
                fail("UnsupportedOperationException should have been thrown");
            } catch (UnsupportedOperationException e) { }
            //  - Edge fields
            assertTrue(raw.isEdge());
            assertEquals(new GraphElement(edge), raw.getGraphElement());
            assertTrue(raw.isDirected());
            assertEquals(sourceType, raw.getSourceType());
            assertEquals(sourceValue, raw.getSourceValue());
            assertEquals(destinationType, raw.getDestinationType());
            assertEquals(destinationValue, raw.getDestinationValue());
            if (count == 0) {
                assertEquals(sourceType, raw.getFirstType());
                assertEquals(sourceValue, raw.getFirstValue());
                assertEquals(destinationType, raw.getSecondType());
                assertEquals(destinationValue, raw.getSecondValue());
            } else {
                assertEquals(destinationType, raw.getFirstType());
                assertEquals(destinationValue, raw.getFirstValue());
                assertEquals(sourceType, raw.getSecondType());
                assertEquals(sourceValue, raw.getSecondValue());
            }
            assertEquals(summaryType, raw.getSummaryType());
            assertEquals(summarySubType, raw.getSummarySubType());
            assertEquals(visibility, raw.getVisibility());
            assertEquals(startDate, raw.getStartDate());
            assertEquals(endDate, raw.getEndDate());
            //  - Statistic fields
            assertEquals(statistics, raw.getSetOfStatistics());
            count++;
        }
    }

    @Test
    public void testUndirectedEdge() throws ParseException {
        String sourceType = "STYPE";
        String sourceValue = "SVALUE";
        String destinationType = "DTYPE";
        String destinationValue = "DVALUE";
        String summaryType = "SUMMARY_TYPE";
        String summarySubType = "SUMMARY_SUB_TYPE";
        String visibility = "ABC";
        Date startDate = DATE_FORMAT.parse("20131027 123456");
        Date endDate = DATE_FORMAT.parse("20131028 123456");
        Edge edge = new Edge(sourceType, sourceValue, destinationType, destinationValue,
                summaryType, summarySubType, false, visibility, startDate, endDate);
        SetOfStatistics statistics = new SetOfStatistics();
        statistics.addStatistic("count", new Count(10));

        Pair<Key> pair = ConversionUtils.getKeysFromGraphElement(new GraphElement(edge));
        Value accValue = ConversionUtils.getValueFromSetOfStatistics(statistics);

        int count = 0;
        for (Key key : new Key[]{pair.getFirst(), pair.getSecond()}) {
            // Create RawGraphElementWithStatistics
            RawGraphElementWithStatistics raw = new RawGraphElementWithStatistics(key, accValue);
            // Check that all fields are correct
            //  - Entity fields
            assertFalse(raw.isEntity());
            try {
                raw.getEntityType();
                fail("UnsupportedOperationException should have been thrown");
            } catch (UnsupportedOperationException e) { }
            try {
                raw.getEntityValue();
                fail("UnsupportedOperationException should have been thrown");
            } catch (UnsupportedOperationException e) { }
            //  - Edge fields
            assertTrue(raw.isEdge());
            assertEquals(new GraphElement(edge), raw.getGraphElement());
            assertFalse(raw.isDirected());
            if (count == 0) {
                assertEquals(destinationType, raw.getSourceType());
                assertEquals(destinationValue, raw.getSourceValue());
                assertEquals(destinationType, raw.getFirstType());
                assertEquals(destinationValue, raw.getFirstValue());
                assertEquals(sourceType, raw.getSecondType());
                assertEquals(sourceValue, raw.getSecondValue());
            } else {
                assertEquals(sourceType, raw.getDestinationType());
                assertEquals(sourceValue, raw.getDestinationValue());
                assertEquals(sourceType, raw.getFirstType());
                assertEquals(sourceValue, raw.getFirstValue());
                assertEquals(destinationType, raw.getSecondType());
                assertEquals(destinationValue, raw.getSecondValue());
            }
            assertEquals(summaryType, raw.getSummaryType());
            assertEquals(summarySubType, raw.getSummarySubType());
            assertEquals(visibility, raw.getVisibility());
            assertEquals(startDate, raw.getStartDate());
            assertEquals(endDate, raw.getEndDate());
            //  - Statistic fields
            assertEquals(statistics, raw.getSetOfStatistics());
            count++;
        }
    }
}

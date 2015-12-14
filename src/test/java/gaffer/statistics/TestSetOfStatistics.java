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
package gaffer.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import gaffer.statistics.impl.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.junit.Test;

/**
 * Tests the merge method in {@link SetOfStatistics}. In particular tests that
 * it does a deep copy when merging statistics in.
 * Also tests write and read methods, and methods for removing statistics.
 */
public class TestSetOfStatistics {

	private static final double DELTA = 1e-10;
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd HHmmss");
	static {
		DATE_FORMAT.setCalendar(new GregorianCalendar(SimpleTimeZone.getTimeZone("UTC"), Locale.UK));
	}

	@Test
	public void basicTestWriteAndRead() {
		// Create a SetOfStatistics
		SetOfStatistics setOfStatistics = new SetOfStatistics();
		setOfStatistics.addStatistic("stat1", new Count(5));
		setOfStatistics.addStatistic("stat2", new Count(10));
		FirstSeen firstSeen = new FirstSeen();
		LastSeen lastSeen = new LastSeen();
		try {
			firstSeen.setFirstSeen(DATE_FORMAT.parse("20131027 123456"));
			lastSeen.setLastSeen(DATE_FORMAT.parse("20131028 172345"));
		} catch (ParseException e) {
			fail("testWriteAndRead failed with ParseException: " + e);
		}
		setOfStatistics.addStatistic("stat3", firstSeen);
		setOfStatistics.addStatistic("stat4", lastSeen);

		try {
			// Write out
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			setOfStatistics.write(out);

			// Read in
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			SetOfStatistics readSetOfStatistics = new SetOfStatistics();
			readSetOfStatistics.readFields(in);

			// Check get the correct result
			assertEquals(setOfStatistics, readSetOfStatistics);
		} catch (IOException e) {
			fail("IOException in testWriteAndRead: " + e);
		}
	}

	@Test
	public void testWriteAndReadWhenEmpty() {
		// Create an empty SetOfStatistics
		SetOfStatistics setOfStatistics = new SetOfStatistics();

		try {
			// Write out
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			setOfStatistics.write(out);

			// Read in
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			SetOfStatistics readSetOfStatistics = new SetOfStatistics();
			readSetOfStatistics.readFields(in);

			// Check get the correct result
			assertEquals(setOfStatistics, readSetOfStatistics);
		} catch (IOException e) {
			fail("IOException in testWriteAndRead: " + e);
		}
	}

	@Test
	public void testWriteAndRead() {
		try {
			// First Statistics
			CappedMinuteCount cappedMinuteCount1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"));
			CappedSetOfStrings cappedSetOfStrings1 = new CappedSetOfStrings(10);
			cappedSetOfStrings1.addString("A");
			Count count1 = new Count(10);
			Count count2 = new Count(12);
			DailyCount dailyCount1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			DoubleCount doubleCount = new DoubleCount(37.0);
			DoubleProduct doubleProduct1 = new DoubleProduct(2.0D);
			DoubleProductViaLogs doubleProductViaLogs1 = new DoubleProductViaLogs(2.0D);
			FirstSeen firstSeen1 = new FirstSeen(DATE_FORMAT.parse("20131027 123456"));
			HourlyBitMap hourlyBitMap1 = new HourlyBitMap(DATE_FORMAT.parse("20131027 123456"));
			HourlyCount hourlyCount1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hourlyCount1.increment(DATE_FORMAT.parse("20140102 124556"), 100L);
			IntArray intArray1 = new IntArray(new int[]{1,2,3});
			IntMax intMax1 = new IntMax(10);
			IntMin intMin1 = new IntMin(5);
			IntSet intSet1 = new IntSet(1, 2, 100, 876);
			LastSeen lastSeen1 = new LastSeen(DATE_FORMAT.parse("20131027 175612"));
			LongCount longCount1 = new LongCount(1234567890L);
			LongMax longMax1 = new LongMax(1000L);
			LongMin longMin1 = new LongMin(5000L);
			Map<Double, Integer> dToIMap1 = new TreeMap<Double, Integer>();
			dToIMap1.put(2.0d, 1);
			MapFromDoubleToCount mapFromDoubleToCount1 = new MapFromDoubleToCount(dToIMap1);
			Map<Integer, Integer> iToIMap1 = new TreeMap<Integer, Integer>();
			iToIMap1.put(2, 1);
			MapFromIntToCount mapFromIntToCount1 = new MapFromIntToCount(iToIMap1);
			Map<Integer, Long> iToLMap1 = new TreeMap<Integer, Long>();
			iToLMap1.put(2010, 1000000000l);
			iToLMap1.put(2011, 2000000000l);
			MapFromIntToLongCount mapFromIntToLongCount1 = new MapFromIntToLongCount(iToLMap1);
			MapFromStringToSetOfStrings mfstsos1 = new MapFromStringToSetOfStrings();
			mfstsos1.add("A", "V1");
			mfstsos1.add("A", "V2");
			mfstsos1.add("B", "V3");
			Map<String, Integer> map1 = new HashMap<String, Integer>();
			map1.put("2010", 1);
			map1.put("2011", 2);
			MapOfCounts mapOfCounts1 = new MapOfCounts(map1);
			Map<String, Long> longmap1 = new HashMap<String, Long>();
			longmap1.put("2010", 1000000000l);
			longmap1.put("2011", 2000000000l);
			MapOfLongCounts mapOfLongCounts1 = new MapOfLongCounts(longmap1);
			MapOfMinuteBitMaps mapOfMinuteBitMaps1 = new MapOfMinuteBitMaps();
			mapOfMinuteBitMaps1.add("A", new MinuteBitMap(new Date(1241244)));
			mapOfMinuteBitMaps1.add("B", new MinuteBitMap(new Date(1000000)));
			MaxDouble maxDouble1 = new MaxDouble(26.2d);
			MinDouble minDouble1 = new MinDouble(1.0d);
			MinuteBitMap minuteBitMap1 = new MinuteBitMap(DATE_FORMAT.parse("20131027 175612"));
			SetOfStrings setOfStrings1 = new SetOfStrings();
			setOfStrings1.addString("ABC");
			ShortMax shortMax1 = new ShortMax((short) 2);
			ShortMin shortMin1 = new ShortMin((short) 10);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("cappedMinuteCount1", cappedMinuteCount1);
			statistics1.addStatistic("cappedSetOfStrings1", cappedSetOfStrings1);
			statistics1.addStatistic("count1", count1);
			statistics1.addStatistic("count2", count2);
			statistics1.addStatistic("dailyCount", dailyCount1);
			statistics1.addStatistic("doubleCount", doubleCount);
			statistics1.addStatistic("doubleProduct", doubleProduct1);
			statistics1.addStatistic("doubleProductViaLogs", doubleProductViaLogs1);
			statistics1.addStatistic("firstSeen", firstSeen1);
			statistics1.addStatistic("hourlyBitMap", hourlyBitMap1);
			statistics1.addStatistic("hourlyCount", hourlyCount1);
			statistics1.addStatistic("intArray", intArray1);
			statistics1.addStatistic("intMax", intMax1);
			statistics1.addStatistic("intMin", intMin1);
			statistics1.addStatistic("intSet", intSet1);
			statistics1.addStatistic("lastSeen", lastSeen1);
			statistics1.addStatistic("longCount", longCount1);
			statistics1.addStatistic("longMax", longMax1);
			statistics1.addStatistic("longMin", longMin1);
			statistics1.addStatistic("mapFromDoubleToCount", mapFromDoubleToCount1);
			statistics1.addStatistic("mapFromIntToCount", mapFromIntToCount1);
			statistics1.addStatistic("mapFromIntToLongCount", mapFromIntToLongCount1);
			statistics1.addStatistic("mapFromStringToSetOfStrings", mfstsos1);
			statistics1.addStatistic("timeSeries", mapOfCounts1);
			statistics1.addStatistic("mapOfLongCounts", mapOfLongCounts1);
			statistics1.addStatistic("mapOfMinuteBitMaps", mapOfMinuteBitMaps1);
			statistics1.addStatistic("maxDouble", maxDouble1);
			statistics1.addStatistic("minDouble", minDouble1);
			statistics1.addStatistic("minuteBitMap", minuteBitMap1);
			statistics1.addStatistic("setOfStrings", setOfStrings1);
			statistics1.addStatistic("shortMax", shortMax1);
			statistics1.addStatistic("shortMin", shortMin1);

			// Second Statistics
			CappedMinuteCount cappedMinuteCount2 = new CappedMinuteCount(100, DATE_FORMAT.parse("20150102 123456"));
			CappedSetOfStrings cappedSetOfStrings2 = new CappedSetOfStrings(10);
			cappedSetOfStrings2.addString("B");
			Count count3 = new Count(35);
			DailyCount dailyCount2 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 50L);
			dailyCount2.increment(DATE_FORMAT.parse("20150102 123456"), 500L);
			DoubleCount doubleCount2 = new DoubleCount(195.0);
			DoubleProduct doubleProduct2 = new DoubleProduct(4.0D);
			DoubleProductViaLogs doubleProductViaLogs2 = new DoubleProductViaLogs(4.0D);
			FirstSeen firstSeen2 = new FirstSeen(DATE_FORMAT.parse("20131027 101723"));
			HourlyBitMap hourlyBitMap2 = new HourlyBitMap(DATE_FORMAT.parse("20131027 101723"));
			HourlyCount hourlyCount2 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hourlyCount2.increment(DATE_FORMAT.parse("20140102 124556"), 100L);
			IntArray intArray2 = new IntArray(new int[]{4,5,6});
			IntMax intMax2 = new IntMax(20);
			IntMin intMin2 = new IntMin(4);
			IntSet intSet2 = new IntSet(1000, 2000, 100, 876);
			LastSeen lastSeen2 = new LastSeen(DATE_FORMAT.parse("20131027 210304"));
			LongCount longCount2 = new LongCount(1234567891L);
			LongMax longMax2 = new LongMax(2000L);
			LongMin longMin2 = new LongMin(10000L);
			Map<Double, Integer> dToIMap2 = new TreeMap<Double, Integer>();
			dToIMap2.put(2.0d, 1);
			dToIMap2.put(3.7d, 2);
			MapFromDoubleToCount mapFromDoubleToCount2 = new MapFromDoubleToCount(dToIMap2);
			Map<Integer, Integer> iToIMap2 = new TreeMap<Integer, Integer>();
			iToIMap2.put(2, 2);
			iToIMap2.put(5, 2);
			MapFromIntToCount mapFromIntToCount2 = new MapFromIntToCount(iToIMap2);
			Map<Integer, Long> iToLMap2 = new TreeMap<Integer, Long>();
			iToLMap2.put(2010, 1000000000l);
			iToLMap2.put(2011, 2000000000l);
			MapFromIntToLongCount mapFromIntToLongCount2 = new MapFromIntToLongCount(iToLMap2);
			MapFromStringToSetOfStrings mfstsos2 = new MapFromStringToSetOfStrings();
			mfstsos2.add("B", "V1");
			mfstsos2.add("C", "V2");
			mfstsos2.add("D", "V3");
			Map<String, Integer> map2 = new HashMap<String, Integer>();
			map2.put("2011", 1);
			map2.put("2012", 2);
			MapOfCounts mapOfCounts2 = new MapOfCounts(map2);
			Map<String, Long> longmap2 = new HashMap<String, Long>();
			longmap2.put("2010", 1000000000l);
			longmap2.put("2011", 2000000000l);
			MapOfLongCounts mapOfLongCounts2 = new MapOfLongCounts(longmap2);
			MapOfMinuteBitMaps mapOfMinuteBitMaps2 = new MapOfMinuteBitMaps();
			mapOfMinuteBitMaps2.add("A", new MinuteBitMap(new Date(1241244)));
			mapOfMinuteBitMaps2.add("B", new MinuteBitMap(new Date(1000000)));
			MaxDouble maxDouble2 = new MaxDouble(26.2d);
			MinDouble minDouble2 = new MinDouble(2.0d);
			MinuteBitMap minuteBitMap2 = new MinuteBitMap(DATE_FORMAT.parse("20131027 210304"));
			SetOfStrings setOfStrings2 = new SetOfStrings();
			setOfStrings2.addString("FGH");
			setOfStrings2.addString("XYZ");
			ShortMax shortMax2 = new ShortMax((short) 4);
			ShortMin shortMin2 = new ShortMin((short) 20);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("cappedMinuteCount2", cappedMinuteCount2);
			statistics2.addStatistic("cappedSetOfStrings2", cappedSetOfStrings2);
			statistics2.addStatistic("stat1", count3);
			statistics2.addStatistic("dailyCount2", dailyCount2);
			statistics2.addStatistic("doubleCount", doubleCount2);
			statistics2.addStatistic("doubleProduct", doubleProduct2);
			statistics2.addStatistic("doubleProductViaLogs", doubleProductViaLogs2);
			statistics2.addStatistic("firstSeen", firstSeen2);
			statistics2.addStatistic("hourlyBitMap", hourlyBitMap2);
			statistics2.addStatistic("hourlyCount", hourlyCount2);
			statistics2.addStatistic("intArray", intArray2);
			statistics2.addStatistic("intMax", intMax2);
			statistics2.addStatistic("intMin", intMin2);
			statistics2.addStatistic("intSet", intSet2);
			statistics2.addStatistic("lastSeen", lastSeen2);
			statistics2.addStatistic("longCount", longCount2);
			statistics2.addStatistic("longMax", longMax2);
			statistics2.addStatistic("longMin", longMin2);
			statistics2.addStatistic("mapFromDoubleToCount", mapFromDoubleToCount2);
			statistics2.addStatistic("mapFromIntToCount", mapFromIntToCount2);
			statistics2.addStatistic("mapFromIntToLongCount", mapFromIntToLongCount2);
			statistics2.addStatistic("mapFromStringToSetOfStrings", mfstsos2);
			statistics2.addStatistic("timeSeries", mapOfCounts2);
			statistics2.addStatistic("mapOfLongCounts", mapOfLongCounts2);
			statistics2.addStatistic("mapOfMinuteBitMaps", mapOfMinuteBitMaps2);
			statistics2.addStatistic("maxDouble", maxDouble2);
			statistics2.addStatistic("minDouble", minDouble2);
			statistics2.addStatistic("minuteBitMap", minuteBitMap2);
			statistics2.addStatistic("setOfStrings", setOfStrings2);
			statistics2.addStatistic("shortMax", shortMax2);
			statistics2.addStatistic("shortMin", shortMin2);

			// Write
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			statistics1.write(out);
			statistics2.write(out);

			// Read
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			SetOfStatistics statistics3 = new SetOfStatistics();
			statistics3.readFields(in);
			SetOfStatistics statistics4 = new SetOfStatistics();
			statistics4.readFields(in);

			// Check
			assertEquals(statistics1, statistics3);
			assertEquals(statistics2, statistics4);

		} catch (IOException e) {
			fail("Exception in testWriteAndRead() " + e);
		} catch (ParseException e) {
			fail("Exception in testWriteAndRead() " + e);
		}
	}

	@Test
	public void testMergeStatistics() {
		try {
			// First Statistics
			CappedMinuteCount cappedMinuteCount1 = new CappedMinuteCount(100, DATE_FORMAT.parse("20140102 123456"));
			CappedSetOfStrings cappedSetOfStrings1 = new CappedSetOfStrings(10);
			cappedSetOfStrings1.addString("A");
			Count count1 = new Count(10);
			Count count2 = new Count(12);
			DailyCount dailyCount1 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			DoubleCount doubleCount = new DoubleCount(37.0);
			DoubleProduct doubleProduct1 = new DoubleProduct(2.0D);
			DoubleProductViaLogs doubleProductViaLogs1 = new DoubleProductViaLogs(2.0D);
			FirstSeen firstSeen1 = new FirstSeen(DATE_FORMAT.parse("20131027 123456"));
			HourlyBitMap hourlyBitMap1 = new HourlyBitMap(DATE_FORMAT.parse("20131027 123456"));
			HourlyCount hourlyCount1 = new HourlyCount(DATE_FORMAT.parse("20140102 123456"), 1L);
			hourlyCount1.increment(DATE_FORMAT.parse("20140102 124556"), 100L);
			HyperLogLogPlusPlus hllpp1 = new HyperLogLogPlusPlus();
			hllpp1.add("A");
			IntArray intArray1 = new IntArray(new int[]{1,2,3});
			IntMax intMax1 = new IntMax(10);
			IntMin intMin1 = new IntMin(5);
			IntSet intSet1 = new IntSet(1, 2, 100, 876);
			LastSeen lastSeen1 = new LastSeen(DATE_FORMAT.parse("20131027 175612"));
			LongCount longCount1 = new LongCount(1000L);
			LongMax longMax1 = new LongMax(1000L);
			LongMin longMin1 = new LongMin(5000L);
			Map<Double, Integer> dToIMap1 = new TreeMap<Double, Integer>();
			dToIMap1.put(2.0d, 1);
			MapFromDoubleToCount mapFromDoubleToCount1 = new MapFromDoubleToCount(dToIMap1);
			Map<Integer, Integer> iToIMap1 = new TreeMap<Integer, Integer>();
			iToIMap1.put(2, 1);
			MapFromIntToCount mapFromIntToCount1 = new MapFromIntToCount(iToIMap1);
			MapFromStringToSetOfStrings mfstsos1 = new MapFromStringToSetOfStrings();
			mfstsos1.add("A", "V1");
			mfstsos1.add("A", "V2");
			mfstsos1.add("B", "V3");
			Map<String, Integer> map1 = new HashMap<String, Integer>();
			map1.put("2010", 1);
			map1.put("2011", 2);
			MapOfCounts mapOfCounts1 = new MapOfCounts(map1);
			Map<Integer, Long> iToLMap1 = new TreeMap<Integer, Long>();
			iToLMap1.put(2010, 1000000000l);
			iToLMap1.put(2011, 2000000000l);
			MapFromIntToLongCount mapFromIntToLongCount1 = new MapFromIntToLongCount(iToLMap1);
			Map<String, Long> longmap1 = new HashMap<String, Long>();
			longmap1.put("2010", 1000000000l);
			longmap1.put("2011", 2000000000l);
			MapOfLongCounts mapOfLongCounts1 = new MapOfLongCounts(longmap1);
			MapOfMinuteBitMaps mapOfMinuteBitMaps1 = new MapOfMinuteBitMaps();
			mapOfMinuteBitMaps1.add("A", new MinuteBitMap(new Date(1241244)));
			mapOfMinuteBitMaps1.add("C", new MinuteBitMap(new Date(1000000)));
			MaxDouble maxDouble1 = new MaxDouble(26.2d);
			MinDouble minDouble1 = new MinDouble(2.0d);
			MinuteBitMap minuteBitMap1 = new MinuteBitMap(DATE_FORMAT.parse("20131027 175612"));
			SetOfStrings setOfStrings1 = new SetOfStrings();
			setOfStrings1.addString("ABC");
			ShortMax shortMax1 = new ShortMax((short) 2);
			ShortMin shortMin1 = new ShortMin((short) 10);
			SetOfStatistics statistics1 = new SetOfStatistics();
			statistics1.addStatistic("cappedMinuteCount", cappedMinuteCount1);
			statistics1.addStatistic("cappedSetOfStrings", cappedSetOfStrings1);
			statistics1.addStatistic("count1", count1);
			statistics1.addStatistic("count2", count2);
			statistics1.addStatistic("dailyCount", dailyCount1);
			statistics1.addStatistic("doubleCount", doubleCount);
			statistics1.addStatistic("doubleProduct", doubleProduct1);
			statistics1.addStatistic("doubleProductViaLogs", doubleProductViaLogs1);
			statistics1.addStatistic("firstSeen", firstSeen1);
			statistics1.addStatistic("hourlyBitMap", hourlyBitMap1);
			statistics1.addStatistic("hyperLogLogPlusPlus", hllpp1);
			statistics1.addStatistic("intArray", intArray1);
			statistics1.addStatistic("intMax", intMax1);
			statistics1.addStatistic("intMin", intMin1);
			statistics1.addStatistic("intSet", intSet1);
			statistics1.addStatistic("lastSeen", lastSeen1);
			statistics1.addStatistic("longCount", longCount1);
			statistics1.addStatistic("longMax", longMax1);
			statistics1.addStatistic("longMin", longMin1);
			statistics1.addStatistic("mapFromDoubleToCount", mapFromDoubleToCount1);
			statistics1.addStatistic("mapFromIntToCount", mapFromIntToCount1);
			statistics1.addStatistic("mapFromIntToLongCount", mapFromIntToLongCount1);
			statistics1.addStatistic("maxDouble", maxDouble1);
			statistics1.addStatistic("minDouble", minDouble1);
			statistics1.addStatistic("mapFromStringToSetOfStrings", mfstsos1);
			statistics1.addStatistic("timeSeries", mapOfCounts1);
			statistics1.addStatistic("mapOfLongCounts", mapOfLongCounts1);
			statistics1.addStatistic("mapOfMinuteBitMaps", mapOfMinuteBitMaps1);
			statistics1.addStatistic("minuteBitMap", minuteBitMap1);
			statistics1.addStatistic("setOfStrings", setOfStrings1);
			statistics1.addStatistic("shortMax", shortMax1);
			statistics1.addStatistic("shortMin", shortMin1);
			
			// Second Statistics
			CappedMinuteCount cappedMinuteCount2 = new CappedMinuteCount(100, DATE_FORMAT.parse("20150102 123456"));
			CappedSetOfStrings cappedSetOfStrings2 = new CappedSetOfStrings(10);
			cappedSetOfStrings2.addString("B");
			Count count3 = new Count(35);
			DailyCount dailyCount2 = new DailyCount(DATE_FORMAT.parse("20140102 123456"), 50L);
			dailyCount2.increment(DATE_FORMAT.parse("20150102 123456"), 500L);
			DoubleCount doubleCount2 = new DoubleCount(195.0);
			DoubleProduct doubleProduct2 = new DoubleProduct(4.0D);
			DoubleProductViaLogs doubleProductViaLogs2 = new DoubleProductViaLogs(4.0D);
			FirstSeen firstSeen2 = new FirstSeen(DATE_FORMAT.parse("20131027 101723"));
			HourlyBitMap hourlyBitMap2 = new HourlyBitMap(DATE_FORMAT.parse("20131027 101723"));
			HourlyCount hourlyCount2 = new HourlyCount(DATE_FORMAT.parse("20130102 123456"), 1L);
			HyperLogLogPlusPlus hllpp2 = new HyperLogLogPlusPlus();
			hllpp2.add("B");
			IntArray intArray2 = new IntArray(new int[]{4,5,6});
			IntMax intMax2 = new IntMax(20);
			IntMin intMin2 = new IntMin(4);
			IntSet intSet2 = new IntSet(1000, 2000, 100, 876);
			LastSeen lastSeen2 = new LastSeen(DATE_FORMAT.parse("20131027 210304"));
			LongCount longCount2 = new LongCount(2000L);
			LongMax longMax2 = new LongMax(2000L);
			LongMin longMin2 = new LongMin(10000L);
			Map<Double, Integer> dToIMap2 = new TreeMap<Double, Integer>();
			dToIMap2.put(2.0d, 1);
			dToIMap2.put(3.7d, 2);
			MapFromDoubleToCount mapFromDoubleToCount2 = new MapFromDoubleToCount(dToIMap2);
			Map<Integer, Integer> iToIMap2 = new TreeMap<Integer, Integer>();
			iToIMap2.put(2, 2);
			iToIMap2.put(5, 2);
			MapFromIntToCount mapFromIntToCount2 = new MapFromIntToCount(iToIMap2);
			Map<Integer, Long> iToLMap2 = new TreeMap<Integer, Long>();
			iToLMap2.put(2011, 1000000000L);
			iToLMap2.put(2012, 2000000000L);
			MapFromIntToLongCount mapFromIntToLongCount2 = new MapFromIntToLongCount(iToLMap2);
			MapFromStringToSetOfStrings mfstsos2 = new MapFromStringToSetOfStrings();
			mfstsos2.add("B", "V1");
			mfstsos2.add("C", "V2");
			mfstsos2.add("D", "V3");
			Map<String, Integer> map2 = new HashMap<String, Integer>();
			map2.put("2011", 1);
			map2.put("2012", 2);
			MapOfCounts mapOfCounts2 = new MapOfCounts(map2);
			Map<String, Long> longmap2 = new HashMap<String, Long>();
			longmap2.put("2011", 1000000000L);
			longmap2.put("2012", 2000000000L);
			MapOfLongCounts mapOfLongCounts2 = new MapOfLongCounts(longmap2);
			MapOfMinuteBitMaps mapOfMinuteBitMaps2 = new MapOfMinuteBitMaps();
			mapOfMinuteBitMaps2.add("C", new Date(1100000));
			mapOfMinuteBitMaps2.add("D", new Date(9999999));
			MaxDouble maxDouble2 = new MaxDouble(123.1d);
			MinDouble minDouble2 = new MinDouble(1.0d);
			MinuteBitMap minuteBitMap2 = new MinuteBitMap(DATE_FORMAT.parse("20131027 210304"));
			SetOfStrings setOfStrings2 = new SetOfStrings();
			setOfStrings2.addString("FGH");
			setOfStrings2.addString("XYZ");
			ShortMax shortMax2 = new ShortMax((short) 4);
			ShortMin shortMin2 = new ShortMin((short) 20);
			SetOfStatistics statistics2 = new SetOfStatistics();
			statistics2.addStatistic("cappedMinuteCount", cappedMinuteCount2);
			statistics2.addStatistic("cappedSetOfStrings", cappedSetOfStrings2);
			statistics2.addStatistic("count1", count3);
			statistics2.addStatistic("dailyCount", dailyCount2);
			statistics2.addStatistic("doubleCount", doubleCount2);
			statistics2.addStatistic("doubleProduct", doubleProduct2);
			statistics2.addStatistic("doubleProductViaLogs", doubleProductViaLogs2);
			statistics2.addStatistic("firstSeen", firstSeen2);
			statistics2.addStatistic("hourlyBitMap", hourlyBitMap2);
			statistics2.addStatistic("hourlyCount", hourlyCount2);
			statistics2.addStatistic("hyperLogLogPlusPlus", hllpp2);
			statistics2.addStatistic("intArray", intArray2);
			statistics2.addStatistic("intMax", intMax2);
			statistics2.addStatistic("intMin", intMin2);
			statistics2.addStatistic("intSet", intSet2);
			statistics2.addStatistic("lastSeen", lastSeen2);
			statistics2.addStatistic("longCount", longCount2);
			statistics2.addStatistic("longMax", longMax2);
			statistics2.addStatistic("longMin", longMin2);
			statistics2.addStatistic("mapFromIntToCount", mapFromIntToCount2);
			statistics2.addStatistic("mapFromDoubleToCount", mapFromDoubleToCount2);
			statistics2.addStatistic("mapFromIntToLongCount", mapFromIntToLongCount2);
			statistics2.addStatistic("mapFromStringToSetOfStrings", mfstsos2);
			statistics2.addStatistic("timeSeries", mapOfCounts2);
			statistics2.addStatistic("mapOfLongCounts", mapOfLongCounts2);
			statistics2.addStatistic("mapOfMinuteBitMaps", mapOfMinuteBitMaps2);
			statistics2.addStatistic("maxDouble", maxDouble2);
			statistics2.addStatistic("minDouble", minDouble2);
			statistics2.addStatistic("minuteBitMap", minuteBitMap2);
			statistics2.addStatistic("setOfStrings", setOfStrings2);
			statistics2.addStatistic("shortMax", shortMax2);
			statistics2.addStatistic("shortMin", shortMin2);

			// Merge them
			statistics1.merge(statistics2);
			
			// There should be 33 entries
			assertEquals(33, statistics1.getStatistics().size());
			// cappedMinuteCount should be a CappedMinuteCount
			CappedMinuteCount cmc = new CappedMinuteCount(100);
			cmc.increment(DATE_FORMAT.parse("20140102 123456"));
			cmc.increment(DATE_FORMAT.parse("20150102 123456"));
			assertEquals(cmc, statistics1.getStatisticByName("cappedMinuteCount"));
			// cappedSetOfStrings should be a CappedSetOfStrings with maxSize 10 and containing the strings A and B
			CappedSetOfStrings csos = new CappedSetOfStrings(10);
			csos.addString("A");
			csos.addString("B");
			assertEquals(csos, statistics1.getStatisticByName("cappedSetOfStrings"));
			// stat1 should be a Count with value 45
			assertEquals(new Count(45), statistics1.getStatisticByName("count1"));
			// stat2 should be a Count with value 12
			assertEquals(new Count(12), statistics1.getStatisticByName("count2"));
			// dailyCount should be a DailyCount containing 20140102 -> 51L, 20150102 -> 500L
			DailyCount expectedDailyCount = new DailyCount();
			expectedDailyCount.increment(DATE_FORMAT.parse("20140102 000000"), 51L);
			expectedDailyCount.increment(DATE_FORMAT.parse("20150102 000000"), 500L);
			// doubleCount should be a DoubleCount with value 232.0
			assertEquals((new DoubleCount(232.0)).getCount(), ((DoubleCount) statistics1.getStatisticByName("doubleCount")).getCount(), DELTA);
			// doubleProduct should be a DoubleProduct with value 8.0D
			assertEquals(8.0D, ((DoubleProduct) statistics1.getStatisticByName("doubleProduct")).getProduct(), DELTA);
			// doubleProductViaLogs should be a DoubleProductViaLogs with value 
			assertEquals(8.0D, ((DoubleProductViaLogs) statistics1.getStatisticByName("doubleProductViaLogs")).getProduct(), DELTA);
			// firstSeen should be a FirstSeen with value 20131027 101723
			assertEquals(new FirstSeen(DATE_FORMAT.parse("20131027 101723")), statistics1.getStatisticByName("firstSeen"));
			// hourlyBitMap should contain 2013102712 and 2013102710
			HourlyBitMap expectedHourlyBitMap = new HourlyBitMap(DATE_FORMAT.parse("20131027 123456"));
			expectedHourlyBitMap.add(DATE_FORMAT.parse("20131027 101723"));
			assertEquals(expectedHourlyBitMap, statistics1.getStatisticByName("hourlyBitMap"));
			// hourlyCount should be a HourlyCount containing 2014010212 -> 101L, 2013010112 -> 1L
			HourlyCount expectedHourlyCount = new HourlyCount();
			expectedHourlyCount.increment(DATE_FORMAT.parse("20140102 120000"), 101L);
			expectedHourlyCount.increment(DATE_FORMAT.parse("20130102 120000"), 1L);
			// hyperLogLogPlusPlus should have a cardinality estimate of 2
			assertEquals(2, ((HyperLogLogPlusPlus) statistics1.getStatisticByName("hyperLogLogPlusPlus")).getCardinalityEstimate());
			// intArray should be an IntArray with value {5,7,9}
			assertEquals(new IntArray(new int[]{5,7,9}), statistics1.getStatisticByName("intArray"));
			// intMax should be an IntMax with value 20
			assertEquals(new IntMax(20), statistics1.getStatisticByName("intMax"));
			// intMin should be an IntMin with value 4
			assertEquals(new IntMin(4), statistics1.getStatisticByName("intMin"));
			// intSet should be an IntSet containing 1, 2, 100, 876, 1000, 2000
			assertEquals(new IntSet(1, 2, 100, 876, 1000, 2000), statistics1.getStatisticByName("intSet"));
			// lastSeen should be a LastSeen with value 20131027 210304
			assertEquals(new LastSeen(DATE_FORMAT.parse("20131027 210304")), statistics1.getStatisticByName("lastSeen"));
			// longCount should be a LongCount with value 3000L
			assertEquals(new LongCount(3000L), statistics1.getStatisticByName("longCount"));
			// longMax should be a LongMax with value 2000L
			assertEquals(new LongMax(2000L), statistics1.getStatisticByName("longMax"));
			// longMin should be a LongMin with value 5000L
			assertEquals(new LongMin(5000L), statistics1.getStatisticByName("longMin"));
			// mapFromIntToCount should have 2->3, 5->2
			MapFromIntToCount mfitc = new MapFromIntToCount();
			mfitc.add(2, 3);
			mfitc.add(5, 2);
			assertEquals(mfitc, statistics1.getStatisticByName("mapFromIntToCount"));
			// mapFromDoubleToCount should have 2->2 and 3.7->2
			MapFromDoubleToCount mfdtc = new MapFromDoubleToCount();
			mfdtc.add(2.0d, 2);
			mfdtc.add(3.7d, 2);
			assertEquals(mfdtc, statistics1.getStatisticByName("mapFromDoubleToCount"));
			// mapFromIntToLongCount should be a MapFromIntToLongCount with entries 2010 -> 1000000000, 2011 -> 3000000000, 2012 -> 2000000000
			MapFromIntToLongCount expectedMapFromIntToLongCount = new MapFromIntToLongCount();
			expectedMapFromIntToLongCount.add(2010, 1000000000L);
			expectedMapFromIntToLongCount.add(2011, 3000000000L);
			expectedMapFromIntToLongCount.add(2012, 2000000000L);
			assertEquals(expectedMapFromIntToLongCount, statistics1.getStatisticByName("mapFromIntToLongCount"));
			// mapFromStringToSetOfStrings should be a MapFromStringToSetOfStrings with values 
			MapFromStringToSetOfStrings expectedMapFromStringToSetOfStrings = new MapFromStringToSetOfStrings();
			expectedMapFromStringToSetOfStrings.add("A", "V1");
			expectedMapFromStringToSetOfStrings.add("A", "V2");
			expectedMapFromStringToSetOfStrings.add("B", "V3");
			expectedMapFromStringToSetOfStrings.add("B", "V1");
			expectedMapFromStringToSetOfStrings.add("C", "V2");
			expectedMapFromStringToSetOfStrings.add("D", "V3");
			assertEquals(expectedMapFromStringToSetOfStrings, statistics1.getStatisticByName("mapFromStringToSetOfStrings"));
			// timeSeries should be a MapOfCounts with entries "2010" -> 1, "2011" -> 3, "2012" -> 2
			MapOfCounts expectedMapOfCounts = new MapOfCounts();
			expectedMapOfCounts.add("2010", 1);
			expectedMapOfCounts.add("2011", 3);
			expectedMapOfCounts.add("2012", 2);
			assertEquals(expectedMapOfCounts, statistics1.getStatisticByName("timeSeries"));
			// mapOfLongCounts should be a MapOfLongCounts with entries "2010" -> 1000000000, "2011" -> 3000000000, "2012" -> 2000000000
			MapOfLongCounts expectedMapOfLongCounts = new MapOfLongCounts();
			expectedMapOfLongCounts.add("2010", 1000000000l);
			expectedMapOfLongCounts.add("2011", 3000000000l);
			expectedMapOfLongCounts.add("2012", 2000000000l);
			assertEquals(expectedMapOfLongCounts, statistics1.getStatisticByName("mapOfLongCounts"));
			// mapOfMinuteBitMaps should be a MapOfMinuteBitMaps with entries "A" -> 1241244, "C" -> (1000000, 1100000), "D" ->  9999999
			MapOfMinuteBitMaps expectedMapOfMinuteBitMaps = new MapOfMinuteBitMaps();
			expectedMapOfMinuteBitMaps.add("A", new MinuteBitMap(new Date(1241244)));
			MinuteBitMap bitmap = new MinuteBitMap(new Date(1000000));
			bitmap.add(new Date(1100000));
			expectedMapOfMinuteBitMaps.add("C", bitmap);
			expectedMapOfMinuteBitMaps.add("D", new MinuteBitMap(new Date(9999999)));
			assertEquals(expectedMapOfMinuteBitMaps, statistics1.getStatisticByName("mapOfMinuteBitMaps"));
			// maxDouble should be a MaxDouble with value 123.1
			MaxDouble expectedMaxDouble = new MaxDouble(123.1d);
			assertEquals(expectedMaxDouble, statistics1.getStatisticByName("maxDouble"));
			// minDouble should be a MinDouble with value 1.0
			MinDouble expectedMinDouble = new MinDouble(1.0d);
			assertEquals(expectedMinDouble, statistics1.getStatisticByName("minDouble"));
			// minuteBitMap should be a MinuteBitMap with entries 201310272103, 201310271756
			MinuteBitMap expectedMinuteBitMap = new MinuteBitMap();
			expectedMinuteBitMap.add(DATE_FORMAT.parse("20131027 210300"));
			expectedMinuteBitMap.add(DATE_FORMAT.parse("20131027 175600"));
			assertEquals(expectedMinuteBitMap, statistics1.getStatisticByName("minuteBitMap"));
			// setOfStrings should be a SetOfStrings containing "ABC", "FGH", "XYZ"
			SetOfStrings expectedSetOfStrings = new SetOfStrings();
			expectedSetOfStrings.addString("ABC");
			expectedSetOfStrings.addString("FGH");
			expectedSetOfStrings.addString("XYZ");
			assertEquals(expectedSetOfStrings, statistics1.getStatisticByName("setOfStrings"));
			// shortMax should be a ShortMax with value 4
			ShortMax expectedShortMax = new ShortMax((short) 4);
			assertEquals(expectedShortMax, statistics1.getStatisticByName("shortMax"));
			// shortMin should be a ShortMin with value 10
			ShortMin expectedShortMin = new ShortMin((short) 10);
			assertEquals(expectedShortMin, statistics1.getStatisticByName("shortMin"));
		} catch (ParseException e) {
			fail("Exception in testMergeStatistics " + e);
		}
	}

	@Test
	public void testMergeAndDeepCopy() {
		// Initial statistics
		SetOfStatistics statistics1 = new SetOfStatistics();
		statistics1.addStatistic("count", new Count(1));
		SetOfStatistics statistics2 = new SetOfStatistics();
		statistics2.addStatistic("count", new Count(2));
		SetOfStatistics statistics3 = new SetOfStatistics();
		statistics3.addStatistic("count", new Count(10));
		statistics3.addStatistic("count2", new Count(12));

		// Create merged SetOfStatistics
		SetOfStatistics mergedStatistics = new SetOfStatistics();
		mergedStatistics.merge(statistics1);
		mergedStatistics.merge(statistics2);
		mergedStatistics.merge(statistics3);

		// Manually create correctly merged SetOfStatistics
		SetOfStatistics correctMergedStatistics = new SetOfStatistics();
		correctMergedStatistics.addStatistic("count", new Count(13));
		correctMergedStatistics.addStatistic("count2", new Count(12));

		// Check that merged set is the same as the one we manually calculated
		assertTrue(mergedStatistics.equals(correctMergedStatistics));

		// Check that original statistics have not changed
		SetOfStatistics correctStatistics1 = new SetOfStatistics();
		correctStatistics1.addStatistic("count", new Count(1));
		SetOfStatistics correctStatistics2 = new SetOfStatistics();
		correctStatistics2.addStatistic("count", new Count(2));
		SetOfStatistics correctStatistics3 = new SetOfStatistics();
		correctStatistics3.addStatistic("count", new Count(10));
		correctStatistics3.addStatistic("count2", new Count(12));
		assertEquals(correctStatistics1, statistics1);
		assertEquals(correctStatistics2, statistics2);
		assertEquals(correctStatistics3, statistics3);
	}

	@Test
	public void testClone() {
		// Create SetOfStatistics
		SetOfStatistics statistics = new SetOfStatistics();
		Count count = new Count(1);
		statistics.addStatistic("A", count);

		// Create clone
		SetOfStatistics clonedStatistics = statistics.clone();

		// Check they are equal
		assertEquals(statistics, clonedStatistics);

		// Check the clone is deep
		count = new Count(100);
		assertEquals(new Count(1), clonedStatistics.getStatisticByName("A"));
	}

	@Test
	public void testWriteReadUserDefinedStatistic() {
		SetOfStatistics setOfStatistics = new SetOfStatistics();
		ANewStatistic stat = new ANewStatistic(10);
		setOfStatistics.addStatistic("aNewStat", stat);
		try {
			// Write out
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput out = new DataOutputStream(baos);
			setOfStatistics.write(out);

			// Read in
			DataInput in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
			SetOfStatistics readSetOfStatistics = new SetOfStatistics();
			readSetOfStatistics.readFields(in);

			// Check get the correct result
			assertEquals(setOfStatistics, readSetOfStatistics);
		} catch (IOException e) {
			fail("IOException in testWriteReadUserDefinedStatistic: " + e);
		}
	}

	@Test
	public void testMergeUserDefinedStatistic() {
		SetOfStatistics setOfStatistics1 = new SetOfStatistics();
		ANewStatistic stat1 = new ANewStatistic(10);
		setOfStatistics1.addStatistic("aNewStat1", stat1);
		SetOfStatistics setOfStatistics2 = new SetOfStatistics();
		ANewStatistic stat2 = new ANewStatistic(11);
		setOfStatistics2.addStatistic("aNewStat1", stat2);
		setOfStatistics1.merge(setOfStatistics2);
		Statistic mergedStat = setOfStatistics1.getStatisticByName("aNewStat1");
		assertEquals(new ANewStatistic(21), mergedStat);
	}

	@Test
	public void testRemove() throws ParseException {
		SetOfStatistics setOfStatistics = new SetOfStatistics();
		setOfStatistics.addStatistic("count", new Count(1));
		MinuteBitMap expectedMinuteBitMap = new MinuteBitMap();
		expectedMinuteBitMap.add(DATE_FORMAT.parse("20131027 210300"));
		expectedMinuteBitMap.add(DATE_FORMAT.parse("20131027 175600"));
		setOfStatistics.addStatistic("times", expectedMinuteBitMap);
		Set<String> stats = new HashSet<String>();
		stats.add("count");
		stats.add("times");
		assertEquals(stats, setOfStatistics.getStatistics().keySet());
		setOfStatistics.removeStatistic("times");
		stats = new HashSet<String>();
		stats.add("count");
		assertEquals(stats, setOfStatistics.getStatistics().keySet());
		setOfStatistics.removeStatistic("not_there");
		assertEquals(stats, setOfStatistics.getStatistics().keySet());
	}

	@Test
	public void testKeepOnlyThese() throws ParseException {
		SetOfStatistics setOfStatistics = new SetOfStatistics();
		setOfStatistics.addStatistic("count", new Count(1));
		MinuteBitMap expectedMinuteBitMap = new MinuteBitMap();
		expectedMinuteBitMap.add(DATE_FORMAT.parse("20131027 210300"));
		expectedMinuteBitMap.add(DATE_FORMAT.parse("20131027 175600"));
		setOfStatistics.addStatistic("times", expectedMinuteBitMap);
		Set<String> stats = new HashSet<String>();
		stats.add("count");
		stats.add("times");
		assertEquals(stats, setOfStatistics.getStatistics().keySet());
		setOfStatistics.keepOnlyTheseStatistics(stats);
		assertEquals(stats, setOfStatistics.getStatistics().keySet());
		stats.clear();
		stats.add("count");
		setOfStatistics.keepOnlyTheseStatistics(stats);
		assertEquals(stats, setOfStatistics.getStatistics().keySet());
		setOfStatistics.keepOnlyTheseStatistics(Collections.singleton("not_there"));
		assertEquals(Collections.EMPTY_SET, setOfStatistics.getStatistics().keySet());
	}

	@Test
	public void testRemoveThese() throws ParseException {
		SetOfStatistics setOfStatistics = new SetOfStatistics();
		setOfStatistics.addStatistic("count", new Count(1));
		MinuteBitMap expectedMinuteBitMap = new MinuteBitMap();
		expectedMinuteBitMap.add(DATE_FORMAT.parse("20131027 210300"));
		expectedMinuteBitMap.add(DATE_FORMAT.parse("20131027 175600"));
		setOfStatistics.addStatistic("times", expectedMinuteBitMap);
		Set<String> stats = new HashSet<String>();
		stats.add("count");
		stats.add("times");
		assertEquals(stats, setOfStatistics.getStatistics().keySet());
		setOfStatistics.removeTheseStatistics(Collections.singleton("count"));
		assertEquals(Collections.singleton("times"), setOfStatistics.getStatistics().keySet());
		setOfStatistics.removeTheseStatistics(Collections.singleton("not_there"));
		assertEquals(Collections.singleton("times"), setOfStatistics.getStatistics().keySet());
		setOfStatistics.removeTheseStatistics(stats);
		assertEquals(Collections.EMPTY_SET, setOfStatistics.getStatistics().keySet());
	}

	public static class ANewStatistic implements Statistic {

		private int count;

		public ANewStatistic() {
			count = 0;
		}

		public ANewStatistic(int count) {
			this.count = count;
		}

		@Override
		public void merge(Statistic s) throws IllegalArgumentException {
			if (s instanceof ANewStatistic) {
				count += ((ANewStatistic) s).count;
			} else {
				throw new IllegalArgumentException("Cannot merge a statistic of type " + s.getClass().getName() + " with ANewStatistic");
			}
		}

		@Override
		public Statistic clone() {
			return new ANewStatistic(count);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(count);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readInt();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ANewStatistic that = (ANewStatistic) o;

			if (count != that.count) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return count;
		}
	}

}

package uk.gov.gchq.gaffer.types;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.bitmap.serialisation.json.BitmapJsonModules;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.DoubleSerialiser;
import uk.gov.gchq.gaffer.serialisation.IntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.MapSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.TreeSetStringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawFloatSerialiser;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.time.serialisation.RBMBackedTimestampSetSerialiser;

import java.time.Instant;
import java.util.HashMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class CustomMapTest {

    @Test
    public void shouldJSONSerialiseStringInteger() throws SerialisationException {

        final CustomMap<String, Integer> map = new CustomMap<>(new StringSerialiser(), new IntegerSerialiser());
        map.put("one", 1111);
        map.put("two", 2222);

        final byte[] serialise = JSONSerialiser.serialise(map, true);

        assertEquals(
                "{\n" +
                        "  \"class\" : \"uk.gov.gchq.gaffer.types.CustomMap\",\n" +
                        "  \"keySerialiser\" : {\n" +
                        "    \"class\" : \"uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser\"\n" +
                        "  },\n" +
                        "  \"valueSerialiser\" : {\n" +
                        "    \"class\" : \"uk.gov.gchq.gaffer.serialisation.IntegerSerialiser\"\n" +
                        "  },\n" +
                        "  \"jsonStorage\" : [ {\n" +
                        "    \"uk.gov.gchq.gaffer.commonutil.pair.Pair\" : {\n" +
                        "      \"first\" : \"two\",\n" +
                        "      \"second\" : 2222\n" +
                        "    }\n" +
                        "  }, {\n" +
                        "    \"uk.gov.gchq.gaffer.commonutil.pair.Pair\" : {\n" +
                        "      \"first\" : \"one\",\n" +
                        "      \"second\" : 1111\n" +
                        "    }\n" +
                        "  } ]\n" +
                        "}", new String(serialise));


        final CustomMap deserialise = JSONSerialiser.deserialise(serialise, CustomMap.class);
        assertEquals(map, deserialise);
    }

    @Test
    public void shouldJSONSerialiseBigIntString() throws SerialisationException {

        final CustomMap<TreeSet<String>, Double> map = new CustomMap<>(new TreeSetStringSerialiser(), new DoubleSerialiser());
        final TreeSet<String> key1 = new TreeSet<>();
        key1.add("k1");
        key1.add("k2");
        map.put(key1, 11.11);
        final TreeSet<String> key2 = new TreeSet<>();
        key2.add("k3");
        key2.add("k4");
        map.put(key2, 22.22);

        final byte[] serialise = JSONSerialiser.serialise(map, true);

        assertEquals(
                "{\n" +
                        "  \"class\" : \"uk.gov.gchq.gaffer.types.CustomMap\",\n" +
                        "  \"keySerialiser\" : {\n" +
                        "    \"class\" : \"uk.gov.gchq.gaffer.serialisation.implementation.TreeSetStringSerialiser\"\n" +
                        "  },\n" +
                        "  \"valueSerialiser\" : {\n" +
                        "    \"class\" : \"uk.gov.gchq.gaffer.serialisation.DoubleSerialiser\"\n" +
                        "  },\n" +
                        "  \"jsonStorage\" : [ {\n" +
                        "    \"uk.gov.gchq.gaffer.commonutil.pair.Pair\" : {\n" +
                        "      \"first\" : {\n" +
                        "        \"java.util.TreeSet\" : [ \"k3\", \"k4\" ]\n" +
                        "      },\n" +
                        "      \"second\" : 22.22\n" +
                        "    }\n" +
                        "  }, {\n" +
                        "    \"uk.gov.gchq.gaffer.commonutil.pair.Pair\" : {\n" +
                        "      \"first\" : {\n" +
                        "        \"java.util.TreeSet\" : [ \"k1\", \"k2\" ]\n" +
                        "      },\n" +
                        "      \"second\" : 11.11\n" +
                        "    }\n" +
                        "  } ]\n" +
                        "}", new String(serialise));


        final CustomMap deserialise = JSONSerialiser.deserialise(serialise, CustomMap.class);
        assertEquals(map, deserialise);
    }

    @Test
    public void shouldJSONSerialiseStringMap() throws SerialisationException {


        final MapSerialiser mapSerialiser = new MapSerialiser();
        mapSerialiser.setValueSerialiser(new StringSerialiser());
        mapSerialiser.setKeySerialiser(new StringSerialiser());


        final CustomMap<String, HashMap> map = new CustomMap<>(new StringSerialiser(), mapSerialiser);
        final HashMap<String, String> onem = new HashMap<>();
        onem.put("1one", "111one");
        final HashMap<String, String> twom = new HashMap<>();
        twom.put("2Twoo", "2twwwwo");

        map.put("one", onem);
        map.put("two", twom);

        final byte[] serialise = JSONSerialiser.serialise(map, true);

        assertEquals(
                "{\n" +
                        "  \"class\" : \"uk.gov.gchq.gaffer.types.CustomMap\",\n" +
                        "  \"keySerialiser\" : {\n" +
                        "    \"class\" : \"uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser\"\n" +
                        "  },\n" +
                        "  \"valueSerialiser\" : {\n" +
                        "    \"class\" : \"uk.gov.gchq.gaffer.serialisation.implementation.MapSerialiser\",\n" +
                        "    \"keySerialiser\" : \"uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser\",\n" +
                        "    \"valueSerialiser\" : \"uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser\"\n" +
                        "  },\n" +
                        "  \"jsonStorage\" : [ {\n" +
                        "    \"uk.gov.gchq.gaffer.commonutil.pair.Pair\" : {\n" +
                        "      \"first\" : \"two\",\n" +
                        "      \"second\" : {\n" +
                        "        \"java.util.HashMap\" : {\n" +
                        "          \"2Twoo\" : \"2twwwwo\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }, {\n" +
                        "    \"uk.gov.gchq.gaffer.commonutil.pair.Pair\" : {\n" +
                        "      \"first\" : \"one\",\n" +
                        "      \"second\" : {\n" +
                        "        \"java.util.HashMap\" : {\n" +
                        "          \"1one\" : \"111one\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  } ]\n" +
                        "}", new String(serialise));


        final CustomMap deserialise = JSONSerialiser.deserialise(serialise, CustomMap.class);
        assertEquals(map, deserialise);
    }

    @Test
    public void shouldJSONSerialiseFloatRDM() throws SerialisationException {
        System.setProperty(JSONSerialiser.JSON_SERIALISER_MODULES, BitmapJsonModules.class.getCanonicalName());

        final RBMBackedTimestampSet timestampSet1 = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.MINUTE)
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(11)))
                .build();

        final RBMBackedTimestampSet timestampSet2 = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                .timestamps(Lists.newArrayList(Instant.ofEpochSecond(222222)))
                .build();

        final CustomMap<Float, RBMBackedTimestampSet> map = new CustomMap<>(new RawFloatSerialiser(), new RBMBackedTimestampSetSerialiser());
        map.put(123.3f, timestampSet1);
        map.put(345.6f, timestampSet2);

        final byte[] serialise = JSONSerialiser.serialise(map, true);

        final String expectedString = "{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.CustomMap\",\n" +
                "  \"keySerialiser\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.serialisation.implementation.raw.RawFloatSerialiser\"\n" +
                "  },\n" +
                "  \"valueSerialiser\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.time.serialisation.RBMBackedTimestampSetSerialiser\"\n" +
                "  },\n" +
                "  \"jsonStorage\" : [ {\n" +
                "    \"uk.gov.gchq.gaffer.commonutil.pair.Pair\" : {\n" +
                "      \"first\" : {\n" +
                "        \"java.lang.Float\" : 123.3\n" +
                "      },\n" +
                "      \"second\" : {\n" +
                "        \"uk.gov.gchq.gaffer.time.RBMBackedTimestampSet\" : {\n" +
                "          \"earliest\" : 0.000000000,\n" +
                "          \"latest\" : 0.000000000,\n" +
                "          \"numberOfTimestamps\" : 1,\n" +
                "          \"timeBucket\" : \"MINUTE\",\n" +
                "          \"timestamps\" : [ 0.000000000 ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }, {\n" +
                "    \"uk.gov.gchq.gaffer.commonutil.pair.Pair\" : {\n" +
                "      \"first\" : {\n" +
                "        \"java.lang.Float\" : 345.6\n" +
                "      },\n" +
                "      \"second\" : {\n" +
                "        \"uk.gov.gchq.gaffer.time.RBMBackedTimestampSet\" : {\n" +
                "          \"earliest\" : 219600.000000000,\n" +
                "          \"latest\" : 219600.000000000,\n" +
                "          \"numberOfTimestamps\" : 1,\n" +
                "          \"timeBucket\" : \"HOUR\",\n" +
                "          \"timestamps\" : [ 219600.000000000 ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  } ]\n" +
                "}";

        assertEquals(expectedString, new String(serialise));

        final CustomMap deserialise = JSONSerialiser.deserialise(serialise, CustomMap.class);
        assertEquals(map, deserialise);
    }
}
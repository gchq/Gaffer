package gaffer.function.simple.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import gaffer.exception.SerialisationException;
import gaffer.function.FilterFunction;
import gaffer.function.FilterFunctionTest;
import gaffer.function.Function;
import gaffer.jsonserialisation.JSONSerialiser;

public class MultiRegexTest extends FilterFunctionTest {  

	@Test
	public void shouldAccepValidValue() {
	    // Given
	    final MultiRegex filter = new MultiRegex();
	    filter.addPatternFromString("fail");
	    filter.addPatternFromString("pass");
	    	
	    // When
	    boolean accepted = filter.filter("pass");
	
	    // Then
	    assertTrue(accepted);
	}
	
	@Test
	public void shouldRejectInvalidValue() {
	    // Given
	    final MultiRegex filter = new MultiRegex();
	    filter.addPatternFromString("fail");
	    filter.addPatternFromString("reallyFail");
	    
	    // When
	    boolean accepted = filter.filter("pass");
	
	    // Then
	    assertFalse(accepted);
	}
	
	@Test
	public void shouldClone() {
	    // Given
	    final MultiRegex filter = new MultiRegex();
	
	    // When
	    final MultiRegex clonedFilter = filter.statelessClone();
	
	    // Then
	    assertNotSame(filter, clonedFilter);
	}
	
	@Test
	public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
	    // Given
	    final MultiRegex filter = new MultiRegex();
	    filter.addPatternFromString("test");
	    filter.addPatternFromString("test2");
	
	    // When
	    final String json = new String(new JSONSerialiser().serialise(filter, true));
	
	    // Then
	    assertEquals("{\n" +
	            "  \"class\" : \"gaffer.function.simple.filter.MultiRegex\",\n" +
	            "  \"value\" : [ {\n" +
	            "    \"java.util.regex.Pattern\" : \"test\"\n"+
	            "  }, {\n" +
	            "    \"java.util.regex.Pattern\" : \"test2\"\n"+
	            "  } ]\n"+
	            "}", json);
	
	    // When 2
	    final MultiRegex deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), MultiRegex.class);
	
	    // Then 2
	    assertNotNull(deserialisedFilter);
	}
	
	@Override
	protected FilterFunction getInstance() {
		MultiRegex multi = new MultiRegex();
		multi.addPatternFromString("NOTHING");
		multi.addPatternFromString("[t,T].*[t,T]");
		return multi;
	}
	
	@Override
	protected Object[] getSomeAcceptedInput() {
	    return new Object[]{"testingthisexpressionT"};
	}
	
	@Override
	protected Class<? extends Function> getFunctionClass() {
		return MultiRegex.class;
	}}
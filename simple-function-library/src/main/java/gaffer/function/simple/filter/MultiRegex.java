package gaffer.function.simple.filter;

import java.util.ArrayList;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import gaffer.function.SingleInputFilterFunction;
import gaffer.function.annotation.Inputs;

@Inputs(String.class)
public class MultiRegex extends SingleInputFilterFunction {
    private ArrayList<Pattern> controlValue;
 
    public MultiRegex() {
    	controlValue = new ArrayList<>();
        // Required for serialisations
    }
    
    public MultiRegex(final ArrayList<Pattern> controlValue) {
        this.controlValue = controlValue;
    }
    
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonProperty("value")
    public ArrayList<Pattern> getControlValue() {
        return controlValue;
    }
    
    public void setControlValue(final ArrayList<Pattern> controlValue) {
        this.controlValue = controlValue;
    }
    
    public void addPattern(Pattern pattern) {
    	controlValue.add(pattern);
    }
    
    public void addPatternFromString(String pattern) {
    	controlValue.add(Pattern.compile(pattern));
    }
    
	@Override
	protected boolean filter(Object input) {
		if (null == input || input.getClass() != String.class) {
            return false;
        }
		for(Pattern pattern : controlValue) {
			if(pattern.matcher((CharSequence)input).matches()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public MultiRegex statelessClone() {
		return new MultiRegex(controlValue);
    }

}
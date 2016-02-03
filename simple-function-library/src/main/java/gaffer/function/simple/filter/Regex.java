package gaffer.function.simple.filter;

import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import gaffer.function.SingleInputFilterFunction;
import gaffer.function.annotation.Inputs;

@Inputs(String.class)
public class Regex extends SingleInputFilterFunction {
    private Pattern controlValue;
 
    public Regex() {
        // Required for serialisation
    }

    public Regex(final String controlValue) {
        this(Pattern.compile(controlValue));
    }

    public Regex(final Pattern controlValue) {
        this.controlValue = controlValue;
    }
    
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonProperty("value")
    public Pattern getControlValue() {
        return controlValue;
    }
    
    public void setControlValue(final Pattern controlValue) {
        this.controlValue = controlValue;
    }
    
	@Override
	protected boolean filter(Object input) {
		if (null == input || input.getClass() != String.class) {
            return false;
        }
		return controlValue.matcher((CharSequence)input).matches();
	}

	@Override
	public Regex statelessClone() {
		return new Regex(controlValue);
    }

}
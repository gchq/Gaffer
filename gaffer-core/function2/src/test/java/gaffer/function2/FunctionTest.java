package gaffer.function2;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import gaffer.function2.Function;

import java.io.IOException;

public abstract class FunctionTest {
    private static final ObjectMapper MAPPER = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        return mapper;
    }

    protected String serialise(Object object) throws IOException {
        return MAPPER.writeValueAsString(object);
    }

    protected <T extends Function> T deserialise(String json, Class<T> functionClass) throws IOException {
        return MAPPER.readValue(json, functionClass);
    }
}

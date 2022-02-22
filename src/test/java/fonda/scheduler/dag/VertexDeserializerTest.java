package fonda.scheduler.dag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Test;
import org.springframework.boot.test.autoconfigure.json.JsonTest;

import java.io.IOException;

@JsonTest
public class VertexDeserializerTest {


    @Test
    public void testDeserialize() throws IOException {
        String json = "{\"label\":\"a\", \"type\":\"PROCESS\", \"uid\":0}";
        final ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Vertex.class, new VertexDeserializer());
        objectMapper.registerModule(module);
        final Vertex process = objectMapper.readValue(json, Vertex.class);
        System.out.println( process );
    }

}
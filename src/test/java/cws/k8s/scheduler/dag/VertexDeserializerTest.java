package cws.k8s.scheduler.dag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.json.JsonTest;

import java.io.IOException;

@JsonTest
class VertexDeserializerTest {


    @Test
    void testDeserialize() throws IOException {
        String json = "{\"label\":\"a\", \"type\":\"PROCESS\", \"uid\":0}";
        final ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Vertex.class, new VertexDeserializer());
        objectMapper.registerModule(module);
        final Vertex process = objectMapper.readValue(json, Vertex.class);
        Assertions.assertEquals( "a", process.getLabel() );
        Assertions.assertEquals( Type.PROCESS, process.getType() );
        Assertions.assertEquals( 0, process.getUid() );
        System.out.println( process );
    }

}
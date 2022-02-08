package fonda.scheduler.dag;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.springframework.boot.jackson.JsonComponent;

import java.io.IOException;

@JsonComponent
public class VertexDeserializer extends JsonDeserializer<Vertex> {

    @Override
    public Vertex deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {

        final TreeNode treeNode = jsonParser.getCodec().readTree(jsonParser);

        final Type type = Type.valueOf(((TextNode) treeNode.get("type")).asText());
        final String label = ((TextNode) treeNode.get("label")).asText();
        final int uid =  ((IntNode) treeNode.get("uid")).asInt();

        if ( Type.PROCESS == type ) {
           return new Process( label, uid );
        } else if ( Type.OPERATOR == type ) {
            return new Operator( label, uid );
        } else if ( Type.ORIGIN == type ) {
            return new Origin( label, uid );
        } else {
            throw new IllegalArgumentException( "No implementation for type: " + type );
        }

    }

}

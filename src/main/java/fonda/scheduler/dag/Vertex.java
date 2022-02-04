package fonda.scheduler.dag;

import lombok.Getter;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

@Getter
@ToString
public class Vertex {

    private final String label;
    private final Type type;
    private final int uid;
    private List<Edge> in = new LinkedList<>();
    private List<Edge> out = new LinkedList<>();

    private Vertex() {
        label = null;
        type = null;
        uid = -1;
    }

    public void addInbound( Edge e ) {
        synchronized ( in) {
            in.add(e);
        }
    }

    public void addOutbound( Edge e ) {
        out.add( e );
    }

}

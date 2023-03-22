package cws.k8s.scheduler.dag;

public class Node extends Operator {

    Node(String label, int uid) {
        super(label, uid);
    }

    @Override
    public Type getType() {
        return Type.NODE;
    }
}

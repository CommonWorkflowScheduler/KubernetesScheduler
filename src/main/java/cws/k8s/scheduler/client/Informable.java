package cws.k8s.scheduler.client;

import cws.k8s.scheduler.model.NodeWithAlloc;

public interface Informable {

    void informResourceChange();
    void newNode( NodeWithAlloc node );
    void removedNode( NodeWithAlloc node );


}

package fonda.scheduler.client;

import fonda.scheduler.model.NodeWithAlloc;

public interface Informable {

    void informResourceChange();
    void newNode( NodeWithAlloc node );
    void removedNode( NodeWithAlloc node );


}

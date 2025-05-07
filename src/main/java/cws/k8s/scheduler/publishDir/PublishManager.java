package cws.k8s.scheduler.publishDir;

import java.util.ArrayList;
import java.util.List;

public class PublishManager {

    private final List<PublishItem> publishItems = new ArrayList<>();

    public void addPublishItem( PublishItem publishItem ) {
        publishItems.add( publishItem );
    }

    public int getUnpublishedCount() {
        return publishItems.size();
    }

}

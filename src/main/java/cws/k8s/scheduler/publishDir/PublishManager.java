package cws.k8s.scheduler.publishDir;

import cws.k8s.scheduler.model.location.hierachy.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class PublishManager {

    private final List<PublishItem> publishItems = new ArrayList<>();
    private final HierarchyWrapper hierarchyWrapper;
    private final Path workDir;
    private final Path localDir;
    // This maps contains all published items, key is the source path, value are the destination paths
    // Used to check if symlink target is already published
    private final Map<Path, Set<Path>> publishMap = new HashMap<>();

    public void addPublishItemIntern( PublishItem publishItem ) {
        synchronized( publishItems ) {
            publishItems.add( publishItem );
        }
        addToPublishMap( publishItem );
    }

    private void addToPublishMap( PublishItem publishItem ) {
        synchronized ( publishMap ) {
            final Path source = publishItem.getSource();
            publishMap.compute( source, (k, v) -> {
                if ( v == null ) {
                    return Set.of( publishItem.getDestination() );
                } else {
                    v.add( publishItem.getDestination() );
                    return v;
                }
            } );
        }
    }

    public void addPublishItem( PublishItem item ) {
        final Path source = item.getSource();
        final HierarchyFile file = hierarchyWrapper.getFile( source );
        if ( file instanceof RealHierarchyFile ){
            // Single file
            addPublishItemIntern( item );
        } else if ( file instanceof LinkHierarchyFile linkHierarchyFile ) {
            // Symlink, we need to check if the target is local or remote
            processLinkHierachyFile( item, linkHierarchyFile );
        } else if ( file instanceof Folder folder ) {
            // Folder
            processFolder( item, folder, source );
        } else {
            log.error( "Unknown publish class: {}", file.getClass() );
        }
    }

    private void processFolder( PublishItem item, Folder folder, Path source ) {
        // Add all items in the folder to the publishItems
        addToPublishMap( item );
        for ( Map.Entry<String, HierarchyFile> stringHierarchyFileEntry : folder.getAllChildren().entrySet() ) {
            final String path = stringHierarchyFileEntry.getKey();
            final Path newSrc = source.resolve( path );
            final Path newDst = item.getDestination().resolve( path );
            final PublishItem publishItem = new PublishItem( newSrc, newDst, item.getMode() );
            addPublishItem( publishItem );
        }
    }

    private void processLinkHierachyFile( PublishItem item, LinkHierarchyFile linkHierarchyFile ) {
        // if dst is local, check if dst was already published
        // otherwise add dst to publish list and copy to comparable work dir
        final Path oneDestination;
        if ( linkHierarchyFile.getDst().startsWith( localDir ) ) {
            final Set<Path> paths;
            synchronized ( publishMap ) {
                paths = publishMap.get( linkHierarchyFile.getDst() );
            }
            if ( paths != null ) {
                // The destination is already published, create a symlink to the destination
                oneDestination = paths.iterator().next();
            } else {
                // copy the target to the local work dir, calculate the destination
                final Path relativize = localDir.relativize( linkHierarchyFile.getDst() );
                oneDestination = workDir.resolve( relativize );
                // Copy the target to the local work dir
                addPublishItemIntern( new PublishItem( linkHierarchyFile.getDst(), oneDestination, item.getMode() ) );
            }
        } else {
            // The destination is not local, we do not need to copy it
            oneDestination = linkHierarchyFile.getDst();
        }
        addPublishItemIntern( new PublishItem( item.getDestination(), oneDestination, PublishMode.SYMLINK ) );
    }

    public int getUnpublishedCount() {
        return publishItems.size();
    }

}

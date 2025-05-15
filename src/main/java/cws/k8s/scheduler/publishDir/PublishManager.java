package cws.k8s.scheduler.publishDir;

import cws.k8s.scheduler.client.CWSKubernetesClient;
import cws.k8s.scheduler.model.location.LocationType;
import cws.k8s.scheduler.model.location.NodeLocation;
import cws.k8s.scheduler.model.location.hierachy.*;
import cws.k8s.scheduler.scheduler.SchedulerWithDaemonSet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class PublishManager {

    private final Set<FileItem> openPublishItems = new HashSet<>();
    private final Set<FileItem> processingPublishItems = new HashSet<>();
    private final List<SymlinkItem> symlinkItems = new LinkedList<>();
    private final HierarchyWrapper hierarchyWrapper;
    private final CWSKubernetesClient client;
    private final SchedulerWithDaemonSet scheduler;
    private final Path workDir;
    private final Path localDir;
    // This maps contains all published items, key is the source path, value are the destination paths
    // Used to check if symlink target is already published
    private final Map<Path, Set<Path>> publishMap = new HashMap<>();
    private final PublishExecHolder execHolder = new PublishExecHolder();

    /**
     * Maximum number of arguments for the publish command.
     * Attention: This is not really the maximum number of arguments.
     * The number of arguments is 2 + 2 * MAX_ARGS as two additional arguments are needed for the command
     * and two for each file.
     */
    private final static int MAX_ARGS = 200;

    /**
     * Add a publish item to the list of items to be published.
     * @param publishFile The publish item to be processed
     */
    private void addPublishItemIntern( FileItem publishFile ) {
        synchronized( openPublishItems ) {
            openPublishItems.add( publishFile );
        }
        addToPublishMap( publishFile.item );
    }

    /**
     * Add a publish item to the publish map, this indicates that an item will be published.
     * @param publishItem The publish item to be added
     */
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

    /**
     * Add a publish item to the list of items to be published.
     * @param item The publish item to be processed
     */
    public void addPublishItem( PublishItem item ) {
        final Path source = item.getSource();
        final HierarchyFile file = hierarchyWrapper.getFile( source );
        if ( file instanceof RealHierarchyFile realHierarchyFile ){
            // Single file
            addPublishItemIntern( new FileItem( item, realHierarchyFile ) );
        } else if ( file instanceof LinkHierarchyFile linkHierarchyFile ) {
            // Symlink, we need to check if the target is local or remote
            synchronized ( symlinkItems ) {
                symlinkItems.add( new SymlinkItem( item, linkHierarchyFile ) );
            }
        } else if ( file instanceof Folder folder ) {
            // Folder
            processFolder( item, folder );
        } else {
            log.error( "Unknown publish class: {}", file.getClass() );
        }
    }

    /**
     * Process a folder and add all items in the folder to the publish items.
     * @param item The publish item to be processed
     * @param folder The folder hierarchy to be processed
     */
    private void processFolder( PublishItem item, Folder folder ) {
        // Add all items in the folder to the publishItems
        addToPublishMap( item );
        for ( Map.Entry<String, HierarchyFile> stringHierarchyFileEntry : folder.getAllChildren().entrySet() ) {
            final String path = stringHierarchyFileEntry.getKey();
            final Path newSrc = item.getSource().resolve( path );
            final Path newDst = item.getDestination().resolve( path );
            final PublishItem publishItem = new PublishItem( newSrc, newDst, item.getMode() );
            addPublishItem( publishItem );
        }
    }

    /**
     * Process a link hierarchy file, determine if it can link to an already copied file or if it needs to be copied.
     * @param item The publish item to be processed
     * @param linkHierarchyFile The link hierarchy file to be processed
     * @return The symlink to be created
     */
    private Symlink processLinkHierachyFile( PublishItem item, LinkHierarchyFile linkHierarchyFile ) {
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
                addPublishItem( new PublishItem( linkHierarchyFile.getDst(), oneDestination, item.getMode() ) );
            }
        } else {
            // The destination is not local, we do not need to copy it
            oneDestination = linkHierarchyFile.getDst();
        }
        return new Symlink( item.getDestination(), oneDestination );
    }

    /**
     * Publish the files
     * @param items The items to be published
     * @param node The node to publish the file from
     */
    private void publishFiles( List<FileWrapper> items, NodeLocation node ) {
        if ( items.isEmpty() ) {
            return;
        }

        String[] command = new String[items.size() * 2 + 2];
        command[0] = "publish.sh";
        command[1] = "install -D";

        int i = 2;
        for ( FileWrapper item : items ) {
            final PublishItem publishItem = item.item.item;
            command[i++] = publishItem.getSource().toString();
            command[i++] = publishItem.getDestination().toString();
            synchronized ( processingPublishItems ) {
                synchronized ( openPublishItems ) {
                    openPublishItems.remove( item.item );
                    processingPublishItems.add( item.item );
                }
            }
        }

        String name = "Copying from node: " +  node;
        final List<LocationWrapper> locationWrappers = items.stream().map( FileWrapper::locationWrapper ).toList();
        scheduler.useLocations( locationWrappers );
        final String daemonName = scheduler.getDaemonNameOnNode( node.getIdentifier() );
        final Runnable onFinish = () -> {
            scheduler.freeLocations( locationWrappers );
            synchronized ( processingPublishItems ) {
                items.forEach( fw -> processingPublishItems.remove( fw.item ) );
            }
            execHolder.finishedOnNode( node );
        };
        final PublishListener publishListener = new PublishListener( scheduler, name, onFinish );
        execHolder.addRunnable( node, () ->
            client.execCommand( daemonName, scheduler.getNamespace(), command, publishListener )
        );
    }

    /**
     * Create symlinks to be published
     * @param symlinks The symlinks to be created
     */
    private void createSymlinks( Symlink[] symlinks ) {
        if ( symlinks.length == 0 ) {
            return;
        }
        int start = 0;
        while ( start < symlinks.length ) {
            int end = Math.min( start + MAX_ARGS, symlinks.length );
            createSymlinksIntern( symlinks, start, end );
            start = end;
        }
    }

    private void createSymlinksIntern( final Symlink[] symlinks, final int start, int end ) {
        String[] command = new String[(end - start) * 2 + 2];
        command[0] = "publish.sh";
        command[1] = "ln -s";

        end = Math.min( end, symlinks.length );
        int i = 2;
        for ( int j = start; j < end; j++ ) {
            final Symlink item = symlinks[j];
            command[i++] = item.dst.toString();
            command[i++] = item.src.toString();
        }
        String node = scheduler.getRandomDaemonset();
        final NodeLocation location = NodeLocation.getLocation( node );

        String name = "Copying from node: " +  node;
        final String daemonName = scheduler.getDaemonNameOnNode( node );
        final Runnable onFinish = new InformPublishFinishedRunnable( execHolder, location );
        final PublishListener publishListener = new PublishListener( scheduler, name, onFinish );
        execHolder.addRunnable( location, () ->
                client.execCommand( daemonName, scheduler.getNamespace(), command, publishListener )
        );
    }

    public int getUnpublishedCount() {
        return openPublishItems.size() + processingPublishItems.size() + symlinkItems.size();
    }

    /**
     * Publish items as long as the node has free queue.
     * For every node in nodeItemsMap, take the first items until either the maxSize or MAX_ARGS,
     * start publishing for these items.
     * @param nodeItemsMap A map of all items to be published, grouped by node where the file is located.
     * @param currentlyCopyingTasksOnNode A map of the currently copying tasks on each node
     * @param maxSize The maximum sum of file sizes to be copied withing one run
     * @param maxCopyPerNode The maximum number of files to be copied in one run
     */
    private void publishFirstX( final Map<NodeLocation, LinkedList<FileWrapper>> nodeItemsMap,
                                final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode,
                                final long maxSize,
                                final int maxCopyPerNode ) {
        for ( Map.Entry<NodeLocation, LinkedList<FileWrapper>> entry : nodeItemsMap.entrySet() ) {
            final NodeLocation node = entry.getKey();
            LinkedList<FileWrapper> items = entry.getValue();
            while ( currentlyCopyingTasksOnNode.getOrDefault( node, 0 ) < maxCopyPerNode && items != null && !items.isEmpty() ) {
                currentlyCopyingTasksOnNode.compute( node, ( k, v ) -> v == null ? 1 : v + 1 );
                publishFiles( removeUntil( items, maxSize ), node );
            }
        }
        nodeItemsMap.forEach( ( node, items ) -> publishFiles( items, node ) );
    }

    private List<FileWrapper> removeUntil( LinkedList<FileWrapper> items, long maxSize ) {
        List<FileWrapper> result = new LinkedList<>();
        long currentSize = 0;
        while ( !items.isEmpty() && result.size() < MAX_ARGS && currentSize < maxSize ) {
            final FileWrapper firstItem = items.poll();
            currentSize += firstItem.locationWrapper.getSizeInBytes();
            result.add( firstItem );
        }
        return result;
    }

    /**
     * Trigger to publish some files to the nodes depending on the available copy capacity.
     * @param currentlyCopyingTasksOnNode A map of the currently copying tasks on each node
     * @param maxSize The maximum sum of file sizes to be copied withing one run
     */
    public void triggerPublish( final Map<NodeLocation, Integer> currentlyCopyingTasksOnNode, final long maxSize ) {
        // check for all publish items which node they belong to
        synchronized ( openPublishItems ) {
            final Map<NodeLocation, LinkedList<FileWrapper>> nodeItemsMap = getNodeItemsMap();
            publishFirstX( nodeItemsMap, currentlyCopyingTasksOnNode, maxSize, 1 );
        }
    }

    /**
     * Copy all remaining files
     */
    private void copyAll() {
        log.info( "Triggered: Publish All, {} in queue", openPublishItems.size() );
        synchronized ( openPublishItems ) {
            final Map<NodeLocation, LinkedList<FileWrapper>> nodeItemsMap = getNodeItemsMap();
            publishFirstX( nodeItemsMap, new HashMap<>(), Long.MAX_VALUE, Integer.MAX_VALUE );
        }
    }

    /**
     * Create a Map of all items to be published, grouped by node where the file is located.
     * @return A map of all items to be published, grouped by node where the file is located.
     */
    private Map<NodeLocation, LinkedList<FileWrapper>> getNodeItemsMap() {
        return openPublishItems.parallelStream()
                .map( p -> {
                    final LocationWrapper lastUpdate = p.file.getLastUpdate( LocationType.NODE );
                    if ( lastUpdate != null ) {
                        return new FileWrapper( p, lastUpdate );
                    } else {
                        log.error( "No location found for file: {}", p.item.getSource() );
                        return null;
                    }
                } )
                .filter( Objects::nonNull )
                .collect( Collectors.groupingBy( p -> (NodeLocation) p.locationWrapper.getLocation(), Collectors.toCollection(LinkedList::new) ) );
    }

    /**
     * Finalize the publish process, this will create all symlinks and copy all not yet copied files.
     */
    public void finalizePublish(){
        // Add symlinks at the end when no new real files will be published
        final Symlink[] symlinks;
        synchronized ( symlinkItems ) {
            symlinks = symlinkItems
                    .stream()
                    .map( symlinkItem
                            -> processLinkHierachyFile( symlinkItem.item, symlinkItem.linkHierarchyFile ) )
                    .toArray( Symlink[]::new );
            symlinkItems.clear();
        }
        createSymlinks( symlinks );
        copyAll();
    }

    private record FileItem( PublishItem item, RealHierarchyFile file ) {}

    private record FileWrapper( FileItem item, LocationWrapper locationWrapper ) {}

    private record SymlinkItem(PublishItem item, LinkHierarchyFile linkHierarchyFile) {}

    private record Symlink( Path src, Path dst ) {}

}

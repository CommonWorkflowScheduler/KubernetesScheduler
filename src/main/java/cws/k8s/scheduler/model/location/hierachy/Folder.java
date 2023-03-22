package cws.k8s.scheduler.model.location.hierachy;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class Folder extends HierarchyFile {

    private final ConcurrentMap<String, HierarchyFile> children = new ConcurrentHashMap<>();

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public boolean isSymlink() {
        return false;
    }

    public HierarchyFile get(String name ){
        return children.get( name );
    }

    /**
     * Creates folder if not existing
     * @param name name of the folder to create
     * @return the file with the name, or the new created folder
     */
    public Folder getOrCreateFolder(String name ){
        final HierarchyFile file = children.get(name);
        if( file == null || !file.isDirectory() ){
            return (Folder) children.compute( name, (key,value) -> (value == null || !value.isDirectory()) ? new Folder() : value );
        }
        return (Folder) file;
    }

    public Map<Path, AbstractHierarchyFile> getAllChildren(Path currentPath ){
        Map<Path, AbstractHierarchyFile> result = new TreeMap<>();
        getAllChildren( result, currentPath );
        return result;
    }

    private void getAllChildren(final Map<Path, AbstractHierarchyFile> result, Path currentPath ){
        for (Map.Entry<String, HierarchyFile> entry : children.entrySet()) {
            Path resolve = currentPath.resolve(entry.getKey());
            if ( !entry.getValue().isSymlink() && entry.getValue().isDirectory() ){
                final Folder value = (Folder) entry.getValue();
                value.getAllChildren( result, resolve );
            } else {
                result.put( resolve, (AbstractHierarchyFile) entry.getValue());
            }
        }
    }

    public LocationWrapper addOrUpdateFile(final String name, boolean overwrite, final LocationWrapper location ) {
        final RealHierarchyFile file = (RealHierarchyFile) children.compute( name, (k, v) -> {
            if (v == null || v.isDirectory() || v.isSymlink() ) {
                return new RealHierarchyFile( location );
            }
            return v;
        } );
        return file.addOrUpdateLocation( overwrite, location );
    }

    public boolean addSymlink( final String name, final Path dst ){
        children.compute( name, (k,v) -> {
            if ( v == null || !v.isSymlink() || !((LinkHierarchyFile) v).getDst().equals(dst) ) {
                return new LinkHierarchyFile( dst );
            }
            return v;
        } );
        return true;
    }

}

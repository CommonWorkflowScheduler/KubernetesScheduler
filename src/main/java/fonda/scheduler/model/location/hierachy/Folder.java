package fonda.scheduler.model.location.hierachy;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class Folder extends File {

    private final ConcurrentMap<String, File> children = new ConcurrentHashMap<>();

    Folder(){}

    @Override
    public boolean isDirectory() {
        return true;
    }

    @Override
    public boolean isSymlink() {
        return false;
    }

    public File get( String name ){
        return children.get( name );
    }

    /**
     * Creates folder if not existing
     * @param name name of the folder to create
     * @return the file with the name, or the new created folder
     */
    public Folder getOrCreateFolder(String name ){
        final File file = children.get(name);
        if( file == null || !file.isDirectory() ){
            return (Folder) children.compute( name, (key,value) -> (value == null || !value.isDirectory()) ? new Folder() : value );
        }
        return (Folder) file;
    }

    public Map<Path,AbstractFile> getAllChildren( Path currentPath ){
        Map<Path,AbstractFile> result = new TreeMap<>();
        getAllChildren( result, currentPath );
        return result;
    }

    private void getAllChildren( final Map<Path,AbstractFile> result, Path currentPath ){
        for (Map.Entry<String, File> entry : children.entrySet()) {
            Path resolve = currentPath.resolve(entry.getKey());
            if ( !entry.getValue().isSymlink() && entry.getValue().isDirectory() ){
                final Folder value = (Folder) entry.getValue();
                value.getAllChildren( result, resolve );
            } else {
                result.put( resolve, (AbstractFile) entry.getValue());
            }
        }
    }

    public boolean addOrUpdateFile( final String name, boolean overwrite, final LocationWrapper location ) {
        children.compute( name, (k,v) -> {
            if (v == null || v.isDirectory() || v.isSymlink() )
                return new RealFile( location );
            final RealFile file = (RealFile) v;
            file.addOrUpdateLocation( overwrite, location );
            return v;
        } );
        return true;
    }

    public boolean addSymlink( final String name, final Path dst ){
        children.compute( name, (k,v) -> {
            if ( v == null || !v.isSymlink() || !((LinkFile) v).getDst().equals(dst) )
                return new LinkFile( dst );
            return v;
        } );
        return true;
    }

}

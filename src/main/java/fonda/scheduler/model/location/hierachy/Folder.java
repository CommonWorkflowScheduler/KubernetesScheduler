package fonda.scheduler.model.location.hierachy;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
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

    public Map<Path,RealFile> getAllChildren( Path currentPath ){
        Map<Path,RealFile> result = new TreeMap<>();
        getAllChildren( result, currentPath );
        return result;
    }

    private void getAllChildren( final Map<Path,RealFile> result, Path currentPath ){
        for (Map.Entry<String, File> entry : children.entrySet()) {
            Path resolve = currentPath.resolve(entry.getKey());
            if ( entry.getValue().isDirectory() ){
                final Folder value = (Folder) entry.getValue();
                value.getAllChildren( result, resolve );
            } else {
                result.put( resolve, (RealFile) entry.getValue());
            }
        }
    }

    public boolean addOrUpdateFile( final String name, boolean overwrite, final LocationWrapper... locations ) {
        children.compute( name, (k,v) -> {
            if (v == null || v.isDirectory())
                return new RealFile( locations );
            final RealFile file = (RealFile) v;
            file.addOrUpdateLocation( overwrite, locations );
            return v;
        } );
        return true;
    }

}

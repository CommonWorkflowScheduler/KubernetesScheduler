package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.location.Location;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    public File getFileOrCreateFolder( String name ){
        final File file = children.get(name);
        if( file == null ){
            children.putIfAbsent( name, new Folder() );
            return children.get( name );
        }
        return file;
    }

    public List<Path> getAllChildren(Path currentPath ){
        List<Path> result = new LinkedList<>();
        getAllChildren( result, currentPath );
        return result;
    }

    private void getAllChildren(final List<Path> result, Path currentPath){
        for (Map.Entry<String, File> entry : children.entrySet()) {
            Path resolve = currentPath.resolve(entry.getKey());
            if ( entry.getValue().isDirectory() ){
                final Folder value = (Folder) entry.getValue();
                value.getAllChildren( result, resolve );
            } else {
                result.add( resolve );
            }
        }
    }

    public boolean addFile( String p, long sizeInBytes, Location... locations ) {
        File file = children.get(p);
        if( file == null ){
            children.putIfAbsent( p, new RealFile(sizeInBytes) );
            file = children.get( p );
        }
        if ( !file.isDirectory() ) ((RealFile) file).addLocation( locations );
        return !file.isDirectory();
    }

}

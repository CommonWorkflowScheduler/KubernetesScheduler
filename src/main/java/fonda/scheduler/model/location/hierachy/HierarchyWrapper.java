package fonda.scheduler.model.location.hierachy;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class HierarchyWrapper {

    private final Path workdir;

    private final ConcurrentMap<String, Folder> workDirs = new ConcurrentHashMap<>(2);

    public HierarchyWrapper( String workdir ) {
        this.workdir = Paths.get( workdir ).normalize();
    }

    private Path relativize( String path ){
        return workdir.relativize(Paths.get( path )).normalize();
    }

    private Folder getWorkdir( Iterator<Path> iterator, boolean create ){
        if(!iterator.hasNext()) return null;
        final String hash1 = iterator.next().toString();
        if(!iterator.hasNext()) return null;
        final String hash2 = iterator.next().toString();
        final String key = hash1 + hash2;
        final Folder folder = workDirs.get( key );
        if( create && folder == null ){
            workDirs.putIfAbsent( key, new Folder());
            return workDirs.get( key );
        }
        return folder;
    }

    /**
     *
     * @param path get all files recursively in this folder (absolute path)
     * @return Null if folder is empty, or not found
     */
    public List<Path> getAllFilesInDir( String path ){
        final Path relativePath = relativize( path );
        Iterator<Path> iterator = relativePath.iterator();
        File current = getWorkdir( iterator, false );
        if( current == null ) return null;
        while(iterator.hasNext()){
            Path p = iterator.next();
            if ( current != null && current.isDirectory() ){
                current = ((Folder) current).get( p.toString() );
            } else {
                return null;
            }
        }
        if( current.isDirectory() )
            return ((Folder) current).getAllChildren( Paths.get(path).normalize() );
        else
            return null;
    }

    /**
     *
     * @param path file to add (absolute path)
     * @return false if file can not be created
     */
    public boolean addFile( String path ){
        final Path relativePath = relativize( path );
        if (relativePath.startsWith("..")){
            return false;
        }
        Iterator<Path> iterator = relativePath.iterator();
        Folder current = getWorkdir( iterator, true );
        if( current == null ) return false;
        while(iterator.hasNext()) {
            Path p = iterator.next();
            if( iterator.hasNext() ){
                //folder
                final File file = current.getFileOrCreateFolder( p.toString() );
                if ( !file.isDirectory() ) return false;
                current = (Folder) file;
            } else {
                //file
                return current.addFile( p.toString() );
            }
        }
        //This would add a file in working hierarchy
        return false;
    }
}

package fonda.scheduler.model.location.hierachy;

import fonda.scheduler.model.location.Location;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class HierarchyWrapper {

    private final Path workdir;

    private final ConcurrentMap<String, Folder> workDirs = new ConcurrentHashMap<>(2);

    public HierarchyWrapper( String workdir ) {
        if ( workdir == null ) throw new IllegalArgumentException( "Workdir is not defined" );
        this.workdir = Paths.get( workdir ).normalize();
    }

    private Path relativize( Path path ){
        return workdir.relativize( path ).normalize();
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
    public Map<Path, RealFile> getAllFilesInDir( final Path path ){
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
            return ((Folder) current).getAllChildren( path.normalize() );
        else
            return null;
    }

    /**
     *
     * @param path file to add (absolute path)
     * @param locations locations where the file is located
     * @return false if file can not be created
     */
    public boolean addFile( final Path path, final LocationWrapper... locations ){
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
                current = current.getOrCreateFolder( p.toString() );
            } else {
                //file
                return current.addOrUpdateFile( p.toString(), locations );
            }
        }
        //This would add a file in working hierarchy
        return false;
    }

    /**
     *
     * @param path file to get (absolute path)
     * @return File or null if file does not exist
     */
    public RealFile getFile( Path path ){
        final Path relativePath = relativize( path );
        if (relativePath.startsWith("..")){
            return null;
        }
        Iterator<Path> iterator = relativePath.iterator();
        Folder current = getWorkdir( iterator, true );
        if( current == null ) return null;
        while(iterator.hasNext()) {
            Path p = iterator.next();
            final File file = current.get( p.toString() );
            if( iterator.hasNext() && file.isDirectory() ){
                //folder
                current = (Folder) file;
            } else if ( !iterator.hasNext() && !file.isDirectory() ) {
                //file
                return (RealFile) file;
            } else
                break;
        }
        return null;
    }
}

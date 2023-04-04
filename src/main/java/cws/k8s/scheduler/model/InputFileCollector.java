package cws.k8s.scheduler.model;

import cws.k8s.scheduler.model.location.Location;
import cws.k8s.scheduler.model.location.hierachy.*;
import cws.k8s.scheduler.model.taskinputs.PathFileLocationTriple;
import cws.k8s.scheduler.model.taskinputs.SymlinkInput;
import cws.k8s.scheduler.model.taskinputs.TaskInputs;
import cws.k8s.scheduler.util.Tuple;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class InputFileCollector {

    private final HierarchyWrapper hierarchyWrapper;

    private void processNext(
            final LinkedList<Tuple<HierarchyFile, Path>> toProcess,
            final List<SymlinkInput> symlinks,
            final List<PathFileLocationTriple> files,
            final Set<Location> excludedLocations,
            final Task task
    ) throws NoAlignmentFoundException {
        final Tuple<HierarchyFile, Path> tuple = toProcess.removeLast();
        final HierarchyFile file = tuple.getA();
        if( file == null ) {
            return;
        }
        final Path path = tuple.getB();
        if ( file.isSymlink() ){
            final Path linkTo = ((LinkHierarchyFile) file).getDst();
            symlinks.add( new SymlinkInput( path, linkTo ) );
            toProcess.push( new Tuple<>( hierarchyWrapper.getFile( linkTo ), linkTo ) );
        } else if( file.isDirectory() ){
            final Map<Path, AbstractHierarchyFile> allChildren = ((Folder) file).getAllChildren(path);
            for (Map.Entry<Path, AbstractHierarchyFile> pathAbstractFileEntry : allChildren.entrySet()) {
                toProcess.push( new Tuple<>( pathAbstractFileEntry.getValue(), pathAbstractFileEntry.getKey() ) );
            }
        } else {
            final RealHierarchyFile realFile = (RealHierarchyFile) file;
            try {
                final RealHierarchyFile.MatchingLocationsPair filesForTask = realFile.getFilesForTask(task);
                if ( filesForTask.getExcludedNodes() != null ) {
                    excludedLocations.addAll(filesForTask.getExcludedNodes());
                }
                files.add( new PathFileLocationTriple( path, realFile, filesForTask.getMatchingLocations()) );
            } catch ( NoAlignmentFoundException e ){
                log.error( "No alignment for task: " + task.getConfig().getName() + " path: " + path, e );
                throw e;
            }
        }
    }

    public TaskInputs getInputsOfTask( Task task, int numberNode ) throws NoAlignmentFoundException {

        final List<InputParam<FileHolder>> fileInputs = task.getConfig().getInputs().fileInputs;
        final LinkedList<Tuple<HierarchyFile,Path>> toProcess = filterFilesToProcess( fileInputs );

        final List<SymlinkInput> symlinks = new ArrayList<>( fileInputs.size() );
        final List<PathFileLocationTriple> files = new ArrayList<>( fileInputs.size() );
        final Set<Location> excludedLocations = new HashSet<>();

        while ( !toProcess.isEmpty() && excludedLocations.size() < numberNode ){
            processNext( toProcess, symlinks, files, excludedLocations, task );
        }

        if( excludedLocations.size() == numberNode ) {
            return null;
        }

        return new TaskInputs( symlinks, files, excludedLocations );

    }

    private LinkedList<Tuple<HierarchyFile,Path>> filterFilesToProcess(List<InputParam<FileHolder>> fileInputs ){
        final LinkedList<Tuple<HierarchyFile,Path>> toProcess = new LinkedList<>();
        for ( InputParam<FileHolder> fileInput : fileInputs) {
            final Path path = Path.of(fileInput.value.sourceObj);
            if ( this.hierarchyWrapper.isInScope( path ) ){
                toProcess.add( new Tuple<>(hierarchyWrapper.getFile( path ), path) );
            }
        }
        return toProcess;
    }

}

package cws.k8s.scheduler.util.score;

import cws.k8s.scheduler.model.location.hierachy.HierarchyWrapper;
import cws.k8s.scheduler.scheduler.data.TaskInputsNodes;
import lombok.RequiredArgsConstructor;

import java.io.File;
import java.nio.file.Path;

@RequiredArgsConstructor
public class FileSizeScore implements CalculateScore {

    private final HierarchyWrapper hierarchyWrapper;

    @Override
    public long getScore( TaskInputsNodes taskInputsNodes ) {

        final long filesInSharedFS = taskInputsNodes.getTask().getConfig()
                .getInputs()
                .fileInputs
                .parallelStream()
                .filter( x -> {
                    final Path path = Path.of( x.value.sourceObj );
                    return !this.hierarchyWrapper.isInScope( path );
                } )
                .mapToLong( input -> new File( input.value.sourceObj ).length() )
                .sum();

        return filesInSharedFS + taskInputsNodes.getInputsOfTask().calculateAvgSize() + 1;
    }
}

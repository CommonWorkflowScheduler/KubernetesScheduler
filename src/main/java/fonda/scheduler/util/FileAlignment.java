package fonda.scheduler.util;

import fonda.scheduler.util.inputs.SymlinkInput;

import java.util.List;
import java.util.Map;

public class FileAlignment {

    public final Map<String, List<FilePath>> nodeFileAlignment;
    public final List<SymlinkInput> symlinks;

    public FileAlignment(Map<String, List<FilePath>> nodeFileAlignment, List<SymlinkInput> symlinks) {
        this.nodeFileAlignment = nodeFileAlignment;
        this.symlinks = symlinks;
    }
}

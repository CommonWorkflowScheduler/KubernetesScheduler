package fonda.scheduler.model;

public class FileHolder {

    public final String storePath;
    public final String sourceObj;
    public final String stageName;

    private FileHolder() {
        this.storePath = null;
        this.sourceObj = null;
        this.stageName = null;
    }

    @Override
    public String toString() {
        return "FileHolder{" +
                "sourceObj='" + sourceObj + '\'' +
                '}';
    }

}

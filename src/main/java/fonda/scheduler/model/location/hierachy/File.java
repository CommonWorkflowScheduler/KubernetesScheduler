package fonda.scheduler.model.location.hierachy;

public class File {

    private final static File fileInstance = new File();

    public static File get(){
        return fileInstance;
    }

    public boolean isDirectory(){
        return false;
    }

}

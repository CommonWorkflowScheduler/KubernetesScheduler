package fonda.scheduler.scheduler.copystrategy;

public class FTPstrategy extends CopyStrategy {

    @Override
    String getResource() {
        return "copystrategies/ftp.py";
    }

}
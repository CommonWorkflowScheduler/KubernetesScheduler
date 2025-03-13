package cws.k8s.scheduler.scheduler.copystrategy;

public class FTPstrategy extends CopyStrategy {

    @Override
    String getResource() {
        return "copystrategies/ftp.py";
    }

}
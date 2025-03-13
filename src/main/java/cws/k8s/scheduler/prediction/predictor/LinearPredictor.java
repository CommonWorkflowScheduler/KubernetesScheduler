package cws.k8s.scheduler.prediction.predictor;

import cws.k8s.scheduler.prediction.Predictor;

public interface LinearPredictor extends Predictor {

    double getR();

}

package cws.k8s.scheduler.model.cluster;

import org.apache.commons.collections4.map.HashedMap;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GroupClusterTest {

    @Test
    void jaccard() {
        Set<String> a = Set.of("a", "b", "c");
        Set<String> b = Set.of("a", "b", "c");
        assertEquals(1.0, GroupCluster.calculateJaccardSimilarityCoefficient(a, b));

        a = Set.of("a", "b", "c");
        b = Set.of("a", "b", "d");
        assertEquals(0.6666666666666666, GroupCluster.calculateJaccardSimilarityCoefficient(a, b));

        a = Set.of("a", "b", "c");
        b = Set.of("d", "e", "f");
        assertEquals(0.01, GroupCluster.calculateJaccardSimilarityCoefficient(a, b));
    }

    @Test
    void testTasksNotOnNode() {
        Map<String,Integer> labels = new HashedMap<>();
        for (int i = 0; i < 10; i++) {
            System.out.println( labels.merge( "a", 1, Integer::sum ) );
        }

    }

}
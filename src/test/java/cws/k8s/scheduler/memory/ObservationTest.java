/*
 * Copyright (c) 2023, Florian Friederici. All rights reserved.
 * 
 * This code is free software: you can redistribute it and/or modify it under 
 * the terms of the GNU General Public License as published by the Free 
 * Software Foundation, either version 3 of the License, or (at your option) 
 * any later version.
 * 
 * This code is distributed in the hope that it will be useful, but WITHOUT ANY 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more 
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with 
 * this work. If not, see <https://www.gnu.org/licenses/>. 
 */

package cws.k8s.scheduler.memory;

import static org.junit.Assert.*;

import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * JUnit 4 Tests for Observation Data Class and related utilities
 * 
 * @author Florian Friederici
 *
 */
@Slf4j
public class ObservationTest {

    @Test
    public void testConstructor() {
        log.info(Thread.currentThread().getStackTrace()[1].getMethodName());
        Observation o1 = new Observation(null, null, null, 0, null, null, null, null, 0);
        Observation o2 = Observation.builder()
                .build();
        assertEquals(o1, o2);
    }
    
    // TODO test sanity checks

}

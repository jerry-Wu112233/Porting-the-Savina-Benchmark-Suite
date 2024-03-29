package edu.rice.habanero.benchmarks.randomgraphs

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

class Statistics {
    val latch        = new CountDownLatch(RandomGraphsParam.NumberOfSpawns)
    val linkCount    = new AtomicInteger()
    val releaseCount = new AtomicInteger()
    val pingCount    = new AtomicInteger()
    val noopCount    = new AtomicInteger()

    override def toString: String = {
        s"""
           |Number of actors created:  ${RandomGraphsParam.NumberOfSpawns - latch.getCount}
           |Number of links created:   ${linkCount.get()}
           |Number of links released:  ${releaseCount.get()}
           |Number of pings sent:      ${pingCount.get()}
           |Number of noops performed: ${noopCount.get()}
           |""".stripMargin
    }
}
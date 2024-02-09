package Util

case class ExperimentResults(
                                distribution: String,
                                dataSize: Long,
                                nDims: Int,
                                nPartitions: Int,
                                totalTreeCreationTime: Double,
                                totalTreeCreationIOs: Int,
                                totalNOverflow: Int,
                                totalSkylineTime: Double,
                                aggrSkylineTime: Double,
                                kForDataset: Int,
                                totalTopKTime: Double,
                                aggrTopKTime: Double,
                                kForSkyline: Int,
                                totalSkyTopKTime_Sol1: Double,
                                aggrSkyTopKTime: Double,
                                totalSkyTopKTime_Sol2: Double
)

package Util

case class ExperimentResults(
                                distribution: String,
                                dataSize: Long,
                                nDims: Int,
                                nPartitions: Int,
                                totalTreeCreationTime: Double,
                                totalSkylineTime: Double,
                                mergeSkylineTime: Double,
                                kForDataset: Int,
                                totalTopKTime: Double,
                                mergeTopKTime: Double,
                                kForSkyline: Int,
                                totalSkyTopKTime: Double,
                                mergeSkyTopKTime: Double
)

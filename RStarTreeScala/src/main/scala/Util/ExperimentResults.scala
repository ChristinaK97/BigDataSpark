package Util

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

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

object ExperimentResults {
  implicit val encoder: Encoder[ExperimentResults] = deriveEncoder
  implicit val decoder: Decoder[ExperimentResults] = deriveDecoder
}
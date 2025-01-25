package schemas

import org.apache.spark.sql.types._

object SteamSchema {
  val schema: StructType = StructType(Seq(
    StructField("appID", StringType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("releaseDate", StringType, nullable = true),
    StructField("estimatedOwners", StringType, nullable = true),
    StructField("peakCCU", IntegerType, nullable = true),
    StructField("requiredAge", IntegerType, nullable = true),
    StructField("price", FloatType, nullable = true),
    StructField("dlcCount", IntegerType, nullable = true),
    StructField("longDesc", StringType, nullable = true),
    StructField("shortDesc", StringType, nullable = true),
    StructField("languages", StringType, nullable = true),
    StructField("fullAudioLanguages", StringType, nullable = true),
    StructField("reviews", StringType, nullable = true),
    StructField("headerImage", StringType, nullable = true),
    StructField("website", StringType, nullable = true),
    StructField("supportWeb", StringType, nullable = true),
    StructField("supportEmail", StringType, nullable = true),
    StructField("supportWindows", BooleanType, nullable = true),
    StructField("supportMac", BooleanType, nullable = true),
    StructField("supportLinux", BooleanType, nullable = true),
    StructField("metacriticScore", IntegerType, nullable = true),
    StructField("metacriticURL", StringType, nullable = true),
    StructField("userScore", IntegerType, nullable = true),
    StructField("positive", IntegerType, nullable = true),
    StructField("negative", IntegerType, nullable = true),
    StructField("scoreRank", StringType, nullable = true),
    StructField("achievements", IntegerType, nullable = true),
    StructField("recommendations", IntegerType, nullable = true),
    StructField("notes", StringType, nullable = true),
    StructField("averagePlaytime", IntegerType, nullable = true),
    StructField("averagePlaytime2W", IntegerType, nullable = true),
    StructField("medianPlaytime", IntegerType, nullable = true),
    StructField("medianPlaytime2W", IntegerType, nullable = true),
    StructField("packages", StringType, nullable = true),
    StructField("developers", StringType, nullable = true),
    StructField("publishers", StringType, nullable = true),
    StructField("categories", StringType, nullable = true),
    StructField("genres", StringType, nullable = true),
    StructField("screenshots", StringType, nullable = true),
    StructField("movies", StringType, nullable = true),
    StructField("tags", StringType, nullable = true)
  ))
}
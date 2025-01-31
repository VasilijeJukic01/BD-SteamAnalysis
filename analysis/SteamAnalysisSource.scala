// Databricks notebook source
// MAGIC %md
// MAGIC # Steam Games Analysis

// COMMAND ----------

import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, PolynomialExpansion}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row

// COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))

// COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/tables/steam_games_parquet.zip", "file:/tmp/steam_games_parquet.zip")

// COMMAND ----------

import java.io.{FileInputStream, FileOutputStream}
import java.util.zip.ZipInputStream
import scala.util.Using
import java.io.File

val localZipPath = "/tmp/steam_games_parquet.zip"
val extractDir = "/tmp/steam_games_parquet"

new File(extractDir).mkdirs()

def extractFilesFromZip(zipPath: String, targetDir: String): Int = {
  Using.Manager { use =>
    val zipInputStream = use(new ZipInputStream(new FileInputStream(zipPath)))
    
    Iterator.continually(zipInputStream.getNextEntry)
      .takeWhile(_ != null)
      .filter(entry => !entry.isDirectory && entry.getName.endsWith(".snappy.parquet"))
      .map { entry =>
        println(s"Extracting file: ${entry.getName}")
        val outputPath = s"$targetDir/${entry.getName}"
        println(s"Extracted file path: $outputPath")
        
        val outputFile = new File(outputPath)
        outputFile.getParentFile.mkdirs()
        
        val outputStream = use(new FileOutputStream(outputFile))
        val buffer = new Array[Byte](1024)
        
        Iterator.continually(zipInputStream.read(buffer))
          .takeWhile(_ > 0)
          .foreach(len => outputStream.write(buffer, 0, len))
        
        1
      }.sum
  }.getOrElse {
    println("An error occurred while processing the ZIP file.")
    0
  }
}

val extractedFileCount = extractFilesFromZip(localZipPath, extractDir)
println(s"Total extracted .snappy.parquet files: $extractedFileCount")

// COMMAND ----------

// Schema
val schema: StructType = StructType(Seq(
    StructField("appID", StringType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("releaseDate", StringType, nullable = true),
    StructField("estimatedOwners", StringType, nullable = true),
    StructField("peakCCU", IntegerType, nullable = true),
    StructField("requiredAge", IntegerType, nullable = true),
    StructField("price", FloatType, nullable = true),
    StructField("dlcCount", IntegerType, nullable = true),
    StructField("languages", StringType, nullable = true),
    StructField("fullAudioLanguages", StringType, nullable = true),
    StructField("reviews", StringType, nullable = true),
    StructField("supportWindows", BooleanType, nullable = true),
    StructField("supportMac", BooleanType, nullable = true),
    StructField("supportLinux", BooleanType, nullable = true),
    StructField("metacriticScore", IntegerType, nullable = true),
    StructField("userScore", IntegerType, nullable = true),
    StructField("positive", IntegerType, nullable = true),
    StructField("negative", IntegerType, nullable = true),
    StructField("scoreRank", StringType, nullable = true),
    StructField("achievements", IntegerType, nullable = true),
    StructField("recommendations", IntegerType, nullable = true),
    StructField("averagePlaytime", IntegerType, nullable = true),
    StructField("averagePlaytime2W", IntegerType, nullable = true),
    StructField("medianPlaytime", IntegerType, nullable = true),
    StructField("medianPlaytime2W", IntegerType, nullable = true),
    StructField("packages", StringType, nullable = true),
    StructField("developers", StringType, nullable = true),
    StructField("publishers", StringType, nullable = true),
    StructField("categories", StringType, nullable = true),
    StructField("genres", StringType, nullable = true),
    StructField("tags", StringType, nullable = true)
))

val spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

val parquetFile = spark.read.schema(schema).parquet("file:/tmp/steam_games_parquet/steam-games.parquet")
val data = parquetFile.dropDuplicates()

display(data)

// COMMAND ----------

println(data.count)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Correlation
// MAGIC
// MAGIC We calculate the correlation matrix for numeric columns in a dataset and then visualize the relationships between them. The correlation matrix helps to understand the relationships between features, which can guide feature selection for analysis.

// COMMAND ----------

val numericColumns = data.schema.fields
  .filter(f => f.dataType.isInstanceOf[NumericType])
  .map(_.name)

val assembler = new VectorAssembler()
  .setInputCols(numericColumns)
  .setOutputCol("features")

val assembledData = assembler.transform(data).select("features")

// Correlation Matrix
val correlationMatrix = Correlation.corr(assembledData, "features").head

val correlationMatrixArray = correlationMatrix.getAs[org.apache.spark.ml.linalg.Matrix](0).toArray
val numCols = numericColumns.length

val rows = correlationMatrixArray.grouped(numCols).toSeq.zipWithIndex.map {
  case (row, i) => Row.fromSeq(row.toSeq :+ numericColumns(i))
}

val schema = StructType(
  numericColumns.map(field => StructField(field, DoubleType, nullable = false)) :+
  StructField("feature", StringType, nullable = false)
)

val correlationDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

correlationDF.createOrReplaceTempView("correlation_matrix_table")

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import seaborn as sns
// MAGIC import pandas as pd
// MAGIC
// MAGIC correlation_matrix = spark.sql("SELECT * FROM correlation_matrix_table").toPandas()
// MAGIC correlation_matrix.set_index("feature", inplace=True)
// MAGIC
// MAGIC # Heatmap
// MAGIC plt.figure(figsize=(10, 8))
// MAGIC sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
// MAGIC plt.title("Correlation matrix")
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Analysis 1 - Popularity Analysis
// MAGIC
// MAGIC This analysis combines sentiment, playtime, and popularity metrics to rank Steam games.
// MAGIC
// MAGIC The sentiment analysis is performed to understand player sentiment based on positive and negative reviews. We calculate a sentimentScore as the difference between the positive and negative review counts. Games are then classified into three categories: **Positive** (sentimentScore > 0), **Neutral** (sentimentScore == 0), and **Negative** (sentimentScore < 0).
// MAGIC
// MAGIC Total playtime is a key indicator of player engagement. To calculate this, we sum the averagePlaytime and averagePlaytime2W columns to create a new column called totalPlaytime. This metric provides insights into how much time players are spending on each game.
// MAGIC
// MAGIC The popularity analysis ranks games based on ownership, user scores, and playtime. We extract numeric values from the estimatedOwners column, and then we calculate a popularityScore as a weighted sum of three factors: 40% estimatedOwnersNum, 30% userScore, and 30% totalPlaytime.
// MAGIC
// MAGIC ### Why this analysis?
// MAGIC - Helps stakeholders understand what makes a game successful and supports data-driven decision-making.

// COMMAND ----------

// Sentiment analysis
val sentimentData = data
  .withColumn("sentimentRatio", 
    col("positive") / (col("positive") + col("negative"))
  )
  .withColumn("sentimentCategory", 
    when(col("sentimentRatio") > 0.6, "Positive")
     .when(col("sentimentRatio") >= 0.4, "Neutral")
     .otherwise("Negative")
  )

val playtimeData = sentimentData
  .withColumn("totalPlaytime", col("averagePlaytime") + col("averagePlaytime2W"))

// Popularity Analysis
val popularityAnalysis = playtimeData
  .withColumn("estimatedOwnersNum", F.regexp_extract(F.col("estimatedOwners"), "(\\d+)", 1).cast(IntegerType))
  .withColumn("popularityScore", 
    (F.col("estimatedOwnersNum") * 0.4) + 
    (F.col("userScore") * 0.3) + 
    (F.col("totalPlaytime") * 0.3)
  )
  .select("name", "estimatedOwners", "userScore", "totalPlaytime", "popularityScore", "sentimentCategory")
  .orderBy(F.desc("popularityScore"))

popularityAnalysis.createOrReplaceTempView("popularity_table")

println("Popularity Analysis")
val top10Games = popularityAnalysis.limit(10).toDF()
display(top10Games.select("name", "popularityScore"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Analysis 2 - Welch ANOVA Test on Game Playtime Across Platforms
// MAGIC
// MAGIC This analysis aims to determine whether the average playtime of games differs significantly across different platforms (Windows, Mac, Linux) using ANOVA (Analysis of Variance).
// MAGIC
// MAGIC Each game is categorized based on platform (games with zero price and zero playtime are removed to ensure meaningful comparisons).
// MAGIC
// MAGIC ### Why Welch ANOVA?
// MAGIC - ANOVA is used to test whether the mean average playtime differs across multiple platform groups.
// MAGIC - We are using Welch ANOVA because there are disbalance between samples
// MAGIC - The F-statistic measures variance between groups relative to variance within groups.
// MAGIC - The p-value determines statistical significance (typically, p < 0.05 indicates a significant difference).
// MAGIC
// MAGIC ### Why this analysis?
// MAGIC - Helps determine whether games perform better on specific platforms in terms of playtime.
// MAGIC - If significant differences exist, it may indicate platform-specific issues affecting player retention.
// MAGIC - Understanding platform engagement trends helps in pricing games more effectively.
// MAGIC - Guides decisions on whether developing for multiple platforms improves engagement.

// COMMAND ----------

val platformData = data.withColumn("platform",
  F.when(F.col("supportWindows") === true && F.col("supportMac") === true && F.col("supportLinux") === true, "All Platforms")
   .when(F.col("supportWindows") === true && F.col("supportMac") === true, "Windows & Mac")
   .when(F.col("supportWindows") === true && F.col("supportLinux") === true, "Windows & Linux")
   .when(F.col("supportWindows") === true, "Windows Only")
   .when(F.col("supportMac") === true, "Mac Only")
   .when(F.col("supportLinux") === true, "Linux Only")
   .otherwise("Unknown")
)

val anovaData = platformData.select("name", "price", "averagePlaytime", "platform")
val filteredData = anovaData.filter(F.col("price") > 0 && F.col("averagePlaytime") > 0)

filteredData.createOrReplaceTempView("anova_platform_table")

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC from pyspark.sql import SparkSession
// MAGIC from scipy.stats import f_oneway, levene
// MAGIC import numpy as np
// MAGIC from statsmodels.stats.oneway import anova_oneway
// MAGIC from statsmodels.stats.multicomp import pairwise_tukeyhsd
// MAGIC
// MAGIC spark = SparkSession.builder.getOrCreate()
// MAGIC anova_platform_data = spark.sql("SELECT * FROM anova_platform_table").toPandas()
// MAGIC
// MAGIC # Remove Mac-Only
// MAGIC anova_platform_data = anova_platform_data[anova_platform_data['platform'] != 'Mac Only']
// MAGIC
// MAGIC platform_counts = anova_platform_data['platform'].value_counts()
// MAGIC platform_counts_df = pd.DataFrame({'Platform': platform_counts.index, 'Count': platform_counts.values})
// MAGIC print(platform_counts_df)
// MAGIC
// MAGIC print(anova_platform_data.head())
// MAGIC
// MAGIC groups = [anova_platform_data[anova_platform_data['platform'] == platform]['averagePlaytime']
// MAGIC           for platform in anova_platform_data['platform'].unique()]
// MAGIC
// MAGIC # Levene's Test
// MAGIC levene_stat, levene_p = levene(*groups)
// MAGIC print(f"Levene's Test Statistic: {levene_stat}, P-value: {levene_p}")
// MAGIC
// MAGIC # If p < 0.05, variances are not equal => Go with Welch ANOVA
// MAGIC
// MAGIC # Welch ANOVA test
// MAGIC f_stat, p_value = anova_oneway(anova_platform_data['averagePlaytime'], anova_platform_data['platform'], use_var="unequal")
// MAGIC print(f"Welch ANOVA F-statistic: {f_stat}, P-value: {p_value}")
// MAGIC
// MAGIC # If ANOVA shows significant difference => Go with Tukey HSD**
// MAGIC if p_value < 0.05:
// MAGIC     print("\nPerforming Tukey HSD test since ANOVA is significant...\n")
// MAGIC     tukey_results = pairwise_tukeyhsd(anova_platform_data['averagePlaytime'], anova_platform_data['platform'])
// MAGIC     print(tukey_results)
// MAGIC
// MAGIC result_df = pd.DataFrame({
// MAGIC     "F_statistic": [f_stat],
// MAGIC     "P_value": [p_value]
// MAGIC })
// MAGIC
// MAGIC result_spark_df = spark.createDataFrame(result_df)
// MAGIC result_spark_df.createOrReplaceTempView("anova_platform_results_table")
// MAGIC result_spark_df.show()

// COMMAND ----------

// MAGIC %python
// MAGIC import seaborn as sns
// MAGIC import matplotlib.pyplot as plt
// MAGIC import numpy as np
// MAGIC
// MAGIC # Outlier removal (5th & 95th percentile)
// MAGIC lower_bound, upper_bound = np.percentile(anova_platform_data['averagePlaytime'], [5, 95])
// MAGIC
// MAGIC filtered_data = anova_platform_data[
// MAGIC     (anova_platform_data['averagePlaytime'] >= lower_bound) & 
// MAGIC     (anova_platform_data['averagePlaytime'] <= upper_bound)
// MAGIC ]
// MAGIC
// MAGIC plt.figure(figsize=(10, 6))
// MAGIC
// MAGIC sns.boxplot(
// MAGIC     data=filtered_data, 
// MAGIC     x='platform', 
// MAGIC     y='averagePlaytime',
// MAGIC     showmeans=True,
// MAGIC     meanprops={"marker":"o", "markerfacecolor":"red", "markeredgecolor":"black", "markersize":"7"}
// MAGIC )
// MAGIC
// MAGIC platforms = filtered_data['platform'].unique()
// MAGIC palette = sns.color_palette("viridis", len(platforms))
// MAGIC
// MAGIC plt.figure(figsize=(10, 6))
// MAGIC hist = sns.histplot(
// MAGIC     data=filtered_data, 
// MAGIC     x="averagePlaytime", 
// MAGIC     hue="platform", 
// MAGIC     kde=True, 
// MAGIC     bins=30, 
// MAGIC     alpha=0.6,
// MAGIC     palette=palette
// MAGIC )
// MAGIC
// MAGIC legend_patches = [mpatches.Patch(color=palette[i], label=platforms[i]) for i in range(len(platforms))]
// MAGIC plt.legend(handles=legend_patches, title="Platform", bbox_to_anchor=(1.05, 1), loc='upper left')
// MAGIC
// MAGIC plt.xlabel("Average Playtime (hours)", fontsize=12)
// MAGIC plt.ylabel("Count", fontsize=12)
// MAGIC plt.title("Distribution of Playtime Across Platforms", fontsize=14)
// MAGIC plt.grid(axis='y', linestyle='--', alpha=0.7)
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Box Plot - Distribution of Average Playtime by Platform
// MAGIC This box plot compares the average playtime for different platforms, filtering out extreme outliers (5th–95th percentile).
// MAGIC
// MAGIC ### Observations
// MAGIC - Windows-only and cross-platform games have a wider range of playtime.
// MAGIC - Median playtime is similar across platforms, but the spread and outliers vary significantly.
// MAGIC
// MAGIC ## Histogram - Distribution of Playtime Across Platforms
// MAGIC This histogram shows the distribution of average playtime for different platforms, with each platform's density curve overlaid.
// MAGIC
// MAGIC ### Observations
// MAGIC - Most games have low playtime, with fewer games having very high playtime.
// MAGIC - Multiple peaks (multimodal distribution) suggests different groups of games with varying playtime patterns.
// MAGIC - Some platforms have higher densities in certain playtime ranges (e.g., Mac and Linux seem to have lower distributions).

// COMMAND ----------

// MAGIC %md
// MAGIC # Analysis 3 - Mann-Whitney U Test on Game Playtime Across Genres
// MAGIC
// MAGIC This analysis examines whether there is a statistically significant difference in average playtime between Action and Strategy games using the Mann-Whitney U test.
// MAGIC
// MAGIC Each game is categorized as Action or Strategy based on its genre information. Games with zero playtime are removed for better analysis.
// MAGIC
// MAGIC ### Why the Mann-Whitney U Test?
// MAGIC - The Mann-Whitney U test is a non-parametric test used to compare two independent groups when data is not normally distributed.
// MAGIC - Unlike the t-test, it does not assume equal variances and is suitable for comparing gameplay distributions across genres.
// MAGIC - The U-statistic measures how the ranked values of one group compare to the other, while the p-value determines statistical significance (typically, p < 0.05 indicates a significant difference).
// MAGIC
// MAGIC ### Why this analysis?
// MAGIC - Helps determine whether Action or Strategy games lead to longer play sessions.
// MAGIC - Understanding genre-based playtime trends helps developers and publishers optimize gameplay mechanics, marketing strategies, and monetization models.

// COMMAND ----------

val mwGenreData = data.withColumn("gameType", 
  F.when(F.lower(F.col("genres")).contains("action"), "Action")
   .when(F.lower(F.col("genres")).contains("strategy"), "Strategy")
   .otherwise(null)
)

val filteredMWGenreData = mwGenreData
  .filter(F.col("gameType").isNotNull && F.col("averagePlaytime") > 0)
  .select("name", "gameType", "averagePlaytime")

filteredMWGenreData.createOrReplaceTempView("mann_whitney_genre_table")

// COMMAND ----------

// MAGIC %python
// MAGIC from scipy.stats import mannwhitneyu
// MAGIC import pandas as pd
// MAGIC import numpy as np
// MAGIC from pyspark.sql import SparkSession
// MAGIC
// MAGIC np.random.seed(42)
// MAGIC
// MAGIC spark = SparkSession.builder.getOrCreate()
// MAGIC mw_genre_data = spark.sql("SELECT * FROM mann_whitney_genre_table").toPandas()
// MAGIC
// MAGIC action_games = mw_genre_data[mw_genre_data["gameType"] == "Action"]["averagePlaytime"]
// MAGIC strategy_games = mw_genre_data[mw_genre_data["gameType"] == "Strategy"]["averagePlaytime"]
// MAGIC
// MAGIC action_sample = np.random.choice(action_games, size=len(strategy_games), replace=False)
// MAGIC
// MAGIC print(action_sample.shape, strategy_games.shape)
// MAGIC
// MAGIC u_stat, p_value = mannwhitneyu(action_sample, strategy_games, alternative="two-sided")
// MAGIC
// MAGIC result_df = pd.DataFrame({
// MAGIC     "U_statistic": [u_stat],
// MAGIC     "P_value": [p_value]
// MAGIC })
// MAGIC
// MAGIC result_spark_df = spark.createDataFrame(result_df)
// MAGIC result_spark_df.createOrReplaceTempView("mann_whitney_genre_results")
// MAGIC
// MAGIC result_spark_df.show()

// COMMAND ----------

// MAGIC %python
// MAGIC import numpy as np
// MAGIC import seaborn as sns
// MAGIC import matplotlib.pyplot as plt
// MAGIC
// MAGIC lower_bound, upper_bound = np.percentile(mw_genre_data['averagePlaytime'], [5, 95])
// MAGIC
// MAGIC filtered_data = mw_genre_data[
// MAGIC     (mw_genre_data['averagePlaytime'] >= lower_bound) & 
// MAGIC     (mw_genre_data['averagePlaytime'] <= upper_bound)
// MAGIC ]
// MAGIC
// MAGIC plt.figure(figsize=(10, 6))
// MAGIC
// MAGIC sns.boxplot(
// MAGIC     data=filtered_data, 
// MAGIC     x="gameType", 
// MAGIC     y="averagePlaytime",
// MAGIC     showmeans=True,
// MAGIC     meanprops={"marker":"o", "markerfacecolor":"red", "markeredgecolor":"black", "markersize":"7"}
// MAGIC )
// MAGIC
// MAGIC plt.xlabel("Game Type", fontsize=12)
// MAGIC plt.ylabel("Average Playtime (hours)", fontsize=12)
// MAGIC plt.title("Distribution of Average Playtime by Game Type (Filtered)", fontsize=14)
// MAGIC plt.grid(axis='y', linestyle='--', alpha=0.7)
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %python
// MAGIC import numpy as np
// MAGIC import seaborn as sns
// MAGIC import matplotlib.pyplot as plt
// MAGIC
// MAGIC lower_bound, upper_bound = np.percentile(mw_genre_data['averagePlaytime'], [5, 95])
// MAGIC
// MAGIC filtered_data = mw_genre_data[
// MAGIC     (mw_genre_data['averagePlaytime'] >= lower_bound) & 
// MAGIC     (mw_genre_data['averagePlaytime'] <= upper_bound)
// MAGIC ]
// MAGIC
// MAGIC plt.figure(figsize=(10, 6))
// MAGIC
// MAGIC sns.histplot(
// MAGIC     data=filtered_data, 
// MAGIC     x="averagePlaytime", 
// MAGIC     hue="gameType", 
// MAGIC     bins=50,
// MAGIC     kde=True, 
// MAGIC     alpha=0.6
// MAGIC )
// MAGIC
// MAGIC plt.xlabel("Average Playtime (hours)", fontsize=12)
// MAGIC plt.ylabel("Count", fontsize=12)
// MAGIC plt.title("Distribution of Average Playtime Across Game Types (Filtered)", fontsize=14)
// MAGIC plt.legend(title="Game Type", loc="upper right")
// MAGIC plt.grid(axis='y', linestyle='--', alpha=0.7)
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Box Plot - Distribution of Average Playtime by Game Type
// MAGIC This box plot compares the average playtime between Action and Strategy games, with extreme outliers filtered at the 5th and 95th percentiles.
// MAGIC
// MAGIC ### Observations
// MAGIC - Action games have a lower median playtime compared to Strategy games, suggesting that Strategy players tend to engage in longer gaming sessions.
// MAGIC - The interquartile range (IQR) for Strategy games is wider, meaning that playtime among Strategy players varies significantly.
// MAGIC
// MAGIC ## Histogram - Distribution of Playtime Across Game Types
// MAGIC This histogram illustrates the frequency distribution of playtime for Action and Strategy games.
// MAGIC
// MAGIC ### Observations
// MAGIC - Action games show a much higher peak at low playtime values, suggesting that many Action games engage players for shorter bursts rather than long sessions.
// MAGIC - Strategy games have a more evenly spread distribution, meaning that they tend to sustain player engagement for longer durations.
// MAGIC - Both distributions are right-skewed, meaning that most games have relatively low playtime, but a few games attract extremely dedicated players.

// COMMAND ----------

// MAGIC %md
// MAGIC # Analysis 4 - Comparing Playtime for High and Low Metacritic Scores
// MAGIC
// MAGIC This analysis provides insights into whether higher-rated games are more engaging, helping players to make choices about which games to play.
// MAGIC
// MAGIC ### Analysis
// MAGIC Analysis uses a two-sample t-test to determine if there is a statistically significant difference in average playtime between games with high Metacritic scores (above 75) and those with low Metacritic scores (75 or below). The goal is to understand if higher-rated games tend to have longer player engagement, as measured by average playtime.
// MAGIC
// MAGIC The dataset is split into two groups:
// MAGIC - **High Metacritic Games**: Games with a metacriticScore greater than 75.
// MAGIC - **Low Metacritic Games**: Games with a metacriticScore of 75 or below.
// MAGIC
// MAGIC For each group, the mean and standard deviation of averagePlaytime are calculated. These statistics provide an initial understanding of the central tendency and variability in playtime for both groups.
// MAGIC
// MAGIC The t-statistic is calculated to measure the difference in means relative to the variability in the data.
// MAGIC The p-value is calculated using the t-distribution with the computed degrees of freedom. It represents the probability of observing the calculated t-statistic (or more extreme) under the null hypothesis.
// MAGIC
// MAGIC ### Hypothesis Testing
// MAGIC - **Null Hypothesis (H₀)**: There is no difference in average playtime between high and low Metacritic games.
// MAGIC - **Alternative Hypothesis (H₁)**: There is a significant difference in average playtime between high and low metacritic games.
// MAGIC
// MAGIC A significance level (α) of 0.05 is used to determine whether to reject the null hypothesis.
// MAGIC
// MAGIC ### Results
// MAGIC The high t-statistic and the extremely small p-value indicate that higher-rated games tend to have significantly longer average playtime compared to lower-rated games.
// MAGIC
// MAGIC ### Why this analysis?
// MAGIC - Players can use this data to make informed choices about which games to invest their time in.
// MAGIC - If high Metacritic games lead to longer playtime, publishers can use this to market games as "long-lasting entertainment".

// COMMAND ----------

import org.apache.commons.math3.distribution.TDistribution

val dfFiltered = data
  .withColumn("averagePlaytime", col("averagePlaytime").cast(DoubleType))
  .filter(col("averagePlaytime").isNotNull && col("averagePlaytime") > 0)

// IQR
val quantiles = dfFiltered.stat.approxQuantile("averagePlaytime", Array(0.25, 0.75), 0.01)

val Q1 = quantiles(0)
val Q3 = quantiles(1)
val IQR = Q3 - Q1
val lowerBound = Q1 - 1.5 * IQR
val upperBound = Q3 + 1.5 * IQR

val dfCleaned = dfFiltered.filter(col("averagePlaytime").between(lowerBound, upperBound))

val highMetacriticGames = dfCleaned.filter(col("metacriticScore") > 75)
val lowMetacriticGames = dfCleaned.filter(col("metacriticScore") <= 75)

val highStats = highMetacriticGames.select(mean("averagePlaytime").as("mean"), stddev("averagePlaytime").as("stddev")).first()
val lowStats = lowMetacriticGames.select(mean("averagePlaytime").as("mean"), stddev("averagePlaytime").as("stddev")).first()

val highMean = highStats.getDouble(0)
val highStdDev = highStats.getDouble(1)
val lowMean = lowStats.getDouble(0)
val lowStdDev = lowStats.getDouble(1)

val n1 = highMetacriticGames.count().toDouble
val n2 = lowMetacriticGames.count().toDouble

if (n1 > 1 && n2 > 1) {
  val meanDiff = highMean - lowMean
  val stdError = math.sqrt((math.pow(highStdDev, 2) / n1) + (math.pow(lowStdDev, 2) / n2))
  val tStat = meanDiff / stdError

  val df = math.pow((math.pow(highStdDev, 2) / n1) + (math.pow(lowStdDev, 2) / n2), 2) /
    ((math.pow(highStdDev, 2) / n1) * (math.pow(highStdDev, 2) / n1) / (n1 - 1) +
      (math.pow(lowStdDev, 2) / n2) * (math.pow(lowStdDev, 2) / n2) / (n2 - 1))

  val tDistribution = new TDistribution(df)
  val pValue = 2 * tDistribution.cumulativeProbability(-math.abs(tStat))

  println(f"T-statistic (after outlier removal): $tStat%.4f")
  println(f"Degrees of freedom: $df%.2f")
  println(f"P-value: $pValue%.4e")

  val alpha = 0.05
  if (pValue < alpha) {
      println("We reject the null hypothesis: there is a statistically significant difference.")
  } 
  else println("We cannot reject the null hypothesis: there is no statistically significant difference.")
} 
else println("Sample sizes too small for a meaningful T-test.")

// COMMAND ----------

// MAGIC %md
// MAGIC # Analysis 5 - Linear Regression on Game Reviews and Player Engagement
// MAGIC
// MAGIC The goal is to build a predictive model using linear regression to understand which factors contribute most to the number of positive reviews a game receives.
// MAGIC
// MAGIC The dataset is initially randomly sampled to reduce its size for faster processing, retaining 10% of the original data.
// MAGIC A linear regression model is trained to predict positive reviews based on:
// MAGIC - **recommendations** (strongest correlation with positive reviews)
// MAGIC - **negative reviews** (popular games tend to have both high positive and high negative reviews)
// MAGIC - **peak concurrent users (peakCCU)** (indicating game popularity and visibility)
// MAGIC
// MAGIC The performance of the linear regression model is evaluated using the following metrics:
// MAGIC - **R² Score** - Measures how well the model explains the variance in positive reviews.
// MAGIC - **Mean Absolute Error (MAE)** - Average absolute difference between the predicted and actual values.
// MAGIC - **Mean Squared Error (MSE)** - Average squared difference between the predicted and actual values (giving more weight to larger errors).
// MAGIC
// MAGIC ### Why this analysis?
// MAGIC - Helps developers understand what drives positive reception - whether high peak player count or recommendations are stronger indicators of a game's success.
// MAGIC - Identifies trends in game popularity, useful for predicting the success of upcoming titles.
// MAGIC - Aids publishers in making marketing decisions (e.g. boosting visibility for highly recommended but under-reviewed games).
// MAGIC - Provides insight into user sentiment dynamics. Showing how negative reviews also correlate with game popularity.

// COMMAND ----------

val data_sample = data.sample(false, 0.1)

val data_cleaned = data_sample.select(
  col("recommendations").cast("int"),
  col("positive").cast("int"),
  col("negative").cast("int"),
  col("peakCCU").cast("int")
).filter(
  col("recommendations").isNotNull &&
  col("positive").isNotNull &&
  col("negative").isNotNull &&
  col("peakCCU").isNotNull
)

// Data preparation
val regression_features = Array("recommendations", "negative", "peakCCU")
val assembler = new VectorAssembler()
  .setInputCols(regression_features)
  .setOutputCol("features")

val data_prepared = assembler.transform(data_cleaned).select("features", "positive")

// Polynomial Expansion (Degree 2)
val polyExpansion = new PolynomialExpansion()
  .setInputCol("features")
  .setOutputCol("poly_features")
  .setDegree(2)

val data_poly = polyExpansion.transform(data_prepared)

// Model Training
val regressor = new LinearRegression()
  .setFeaturesCol("poly_features")
  .setLabelCol("positive")

val regression_model = regressor.fit(data_poly)

// Results
println(s"R2 Score: ${regression_model.summary.r2}")
println(s"Mean Absolute Error (MAE): ${regression_model.summary.meanAbsoluteError}")
println(s"Mean Squared Error (MSE): ${regression_model.summary.meanSquaredError}")
println(s"Coefficients: ${regression_model.coefficients}")
println(s"Intercept: ${regression_model.intercept}")

// Predictions
val predictions = regression_model.transform(data_poly)
predictions.createOrReplaceTempView("predictions_table")

// COMMAND ----------

// MAGIC %python
// MAGIC import matplotlib.pyplot as plt
// MAGIC import pandas as pd
// MAGIC
// MAGIC predictions_df = spark.sql("SELECT positive AS real_positive, prediction FROM predictions_table").toPandas()
// MAGIC
// MAGIC plt.figure(figsize=(12, 8))
// MAGIC
// MAGIC plt.scatter(predictions_df["real_positive"], predictions_df["prediction"], alpha=0.6, label="Predictions")
// MAGIC plt.plot([predictions_df["real_positive"].min(), predictions_df["real_positive"].max()], 
// MAGIC          [predictions_df["real_positive"].min(), predictions_df["real_positive"].max()], 
// MAGIC          color="red", label="Perfect Prediction")
// MAGIC
// MAGIC plt.xscale("log")
// MAGIC plt.yscale("log")
// MAGIC
// MAGIC plt.xlim([0, predictions_df["real_positive"].max() * 1.1])  
// MAGIC plt.ylim([0, predictions_df["prediction"].max() * 1.1])  
// MAGIC
// MAGIC plt.xlabel("Real Positive Reviews")
// MAGIC plt.ylabel("Predicted Positive Reviews")
// MAGIC plt.title("Polynomial Regression Predictions vs Actual")
// MAGIC plt.legend()
// MAGIC plt.grid()
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Predicting Game Popularity Using Decision Trees and Random Forests
// MAGIC
// MAGIC The goal of this analysis is to build a classification model that predicts the popularity of a game based on price, recommendations, and playtime.
// MAGIC Using Decision Tree and Random Forest classifiers, we aim to classify games into three categories:
// MAGIC - **Low popularity** (peakCCU < 100)
// MAGIC - **Medium popularity** (100 ≤ peakCCU < 1000)
// MAGIC - **High popularity** (peakCCU ≥ 1000)
// MAGIC
// MAGIC ### Analysis
// MAGIC The dataset is split into 80% training data and 20% testing data to evaluate model performance.
// MAGIC The performance of both models is assessed using:
// MAGIC - Accuracy Score - Measures the percentage of correct classifications.
// MAGIC - Confusion Matrix - Visualizes how well the models classify different popularity levels.
// MAGIC
// MAGIC ### Why?
// MAGIC - Helps in identifying key factors that drive a game’s success.
// MAGIC - Provides better pricing and promotion strategies based on expected popularity.
// MAGIC - Provides insights into which games are likely to achieve high engagement.
// MAGIC - Could assist in developing more accurate personalized game suggestions.

// COMMAND ----------

import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

val spark = SparkSession.builder()
  .appName("DecisionTreeAndRandomForest")
  .config("spark.master", "local")
  .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "200")

val filteredData = data.filter($"peakCCU" > 0)

val labeled_data = filteredData.withColumn("popularity_label", when($"peakCCU" < 100, 0)
  .when($"peakCCU" >= 100 && $"peakCCU" < 1000, 1)
  .otherwise(2))

labeled_data.groupBy("popularity_label").count().show()

val featureCols = Array("price", "recommendations", "positive", "negative", "medianPlaytime")
val assembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")

val prepared_data = assembler.transform(labeled_data)
  .select("features", "popularity_label")
  .withColumnRenamed("popularity_label", "label")

val Array(trainData, testData) = prepared_data.randomSplit(Array(0.8, 0.2), seed = 1234L)

val decisionTree = new DecisionTreeClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")

val dtModel = decisionTree.fit(trainData)
val dtPredictions = dtModel.transform(testData)

// Evaluation
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val dtAccuracy = evaluator.evaluate(dtPredictions)
println(s"Decision Tree Accuracy: $dtAccuracy")
println(s"Learned Decision Tree Model:\n ${dtModel.toDebugString}")

// Random Forest
val randomForest = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setNumTrees(20)

val rfModel = randomForest.fit(trainData)
val rfPredictions = rfModel.transform(testData)

val rfAccuracy = evaluator.evaluate(rfPredictions)
println(s"Random Forest Accuracy: $rfAccuracy")

println(s"Feature Importances: ${rfModel.featureImportances}")

rfPredictions.createOrReplaceTempView("rf_predictions_table")


// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC import matplotlib.pyplot as plt
// MAGIC from pyspark.sql import SparkSession
// MAGIC
// MAGIC spark = SparkSession.builder.getOrCreate()
// MAGIC rf_predictions = spark.sql("SELECT label, prediction FROM rf_predictions_table").toPandas()
// MAGIC
// MAGIC # Confusion Matrix
// MAGIC from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
// MAGIC
// MAGIC cm = confusion_matrix(rf_predictions['label'], rf_predictions['prediction'])
// MAGIC disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["Low", "Medium", "High"])
// MAGIC disp.plot(cmap="Blues")
// MAGIC plt.title("Random Forest Confusion Matrix")
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %python
// MAGIC import seaborn as sns
// MAGIC import matplotlib.pyplot as plt
// MAGIC
// MAGIC spark = SparkSession.builder.getOrCreate()
// MAGIC df = spark.sql("SELECT label FROM rf_predictions_table").toPandas()
// MAGIC
// MAGIC plt.figure(figsize=(8, 5))
// MAGIC sns.countplot(x=df["label"], palette="viridis")
// MAGIC
// MAGIC plt.xlabel("Popularity Label", fontsize=12)
// MAGIC plt.ylabel("Count", fontsize=12)
// MAGIC plt.title("Distribution of Popularity Labels in the Dataset", fontsize=14)
// MAGIC plt.grid(axis='y', linestyle='--', alpha=0.7)
// MAGIC plt.show()

package mapper

import utils.StringExtensions.RichString

object SteamDataMapper {
  def mapFields(rawData: Map[String, Any]): Map[String, Any] = {
    rawData.map {
      case ("AppID", value) => "appID" -> value
      case ("Name", value) => "name" -> value
      case ("Release date", value) => "releaseDate" -> value
      case ("Estimated owners", value) => "estimatedOwners" -> value
      case ("Peak CCU", value) => "peakCCU" -> value.toString.toIntOption.getOrElse(0)
      case ("Required age", value) => "requiredAge" -> value.toString.toIntOption.getOrElse(0)
      case ("Price", value) => "price" -> value.toString.toFloatOption.getOrElse(0.0f)
      case ("DLC count", value) => "dlcCount" -> value.toString.toIntOption.getOrElse(0)
      case ("About the game", value) => "longDesc" -> value
      case ("Game Features", value) => "shortDesc" -> value
      case ("Supported languages", value) => "languages" -> value
      case ("Full audio languages", value) => "fullAudioLanguages" -> value
      case ("Reviews", value) => "reviews" -> value
      case ("Header image", value) => "headerImage" -> value
      case ("Website", value) => "website" -> value
      case ("Support url", value) => "supportWeb" -> value
      case ("Support email", value) => "supportEmail" -> value
      case ("Windows", value) => "supportWindows" -> value.toString.toBooleanOption.getOrElse(false)
      case ("Mac", value) => "supportMac" -> value.toString.toBooleanOption.getOrElse(false)
      case ("Linux", value) => "supportLinux" -> value.toString.toBooleanOption.getOrElse(false)
      case ("Metacritic score", value) => "metacriticScore" -> value.toString.toIntOption.getOrElse(0)
      case ("Metacritic url", value) => "metacriticURL" -> value
      case ("User score", value) => "userScore" -> value.toString.toIntOption.getOrElse(0)
      case ("Positive", value) => "positive" -> value.toString.toIntOption.getOrElse(0)
      case ("Negative", value) => "negative" -> value.toString.toIntOption.getOrElse(0)
      case ("Score rank", value) => "scoreRank" -> value
      case ("Achievements", value) => "achievements" -> value.toString.toIntOption.getOrElse(0)
      case ("Recommendations", value) => "recommendations" -> value.toString.toIntOption.getOrElse(0)
      case ("Notes", value) => "notes" -> value
      case ("Average playtime forever", value) => "averagePlaytime" -> value.toString.toIntOption.getOrElse(0)
      case ("Average playtime two weeks", value) => "averagePlaytime2W" -> value.toString.toIntOption.getOrElse(0)
      case ("Median playtime forever", value) => "medianPlaytime" -> value.toString.toIntOption.getOrElse(0)
      case ("Median playtime two weeks", value) => "medianPlaytime2W" -> value.toString.toIntOption.getOrElse(0)
      case ("Developers", value) => "developers" -> value
      case ("Publishers", value) => "publishers" -> value
      case ("Categories", value) => "categories" -> value
      case ("Genres", value) => "genres" -> value
      case ("Screenshots", value) => "screenshots" -> value
      case ("Movies", value) => "movies" -> value
      case ("Tags", value) => "tags" -> value
      case other => other
    }
  }
}
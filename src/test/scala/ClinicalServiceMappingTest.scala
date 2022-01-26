
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.junit.Assert._
import org.junit.Test
import org.scalatest.funsuite.AnyFunSuite
import savana.ClinicalService._

/** For the testing part I am going to use JUnit.
 * Functions are the same as in the script but not UDF
 */
class ClinicalServiceMappingTest extends AnyFunSuite{

  Logger.getLogger("org").setLevel(Level.ERROR)

  /** All tests have the same structure:
   * Create spark context
   * Load data as rdd an parse it with flatmap using "," as delimitator
   * Iterate over the rdd in order to check with assert if the transformed data matches or not the regular expresion
   */
  @Test
    def dateTest(): Unit = {

      val sc = new SparkContext("local[*]", "ClinicalServiceMappingTest")

      val inputDates = sc
        .textFile("data/test_date.csv")
        .flatMap(data => data.split(","))
        .collect()

      for (testData <- inputDates) { assertTrue(dateFix(testData).matches("\\d{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[0-1])")) }

      sc.stop()
    }

  @Test
  def genderTest(): Unit = {

    val sc = new SparkContext("local[*]", "ClinicalServiceMappingTest")

    val inputGender = sc
      .textFile("data/test_gender.csv")
      .flatMap(data => data.split(","))
      .collect()

    for (testData <- inputGender) { assertTrue(genderFix(testData).matches("male|female|unknown")) }

    sc.stop()
  }

  @Test
  def quotesTest(): Unit = {

    val sc = new SparkContext("local[*]", "ClinicalServiceMappingTest")

    val inputQuotes = sc
      .textFile("data/test_quotes.csv")
      .flatMap(data => data.split(","))
      .collect()

    for (testData <- inputQuotes) { assertFalse(quotesFix(testData).matches("(\"|”|”).*(\"|”|”)")) }

    sc.stop()
  }

}
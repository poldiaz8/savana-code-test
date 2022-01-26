package savana
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.util.Date
import org.apache.spark.sql.types.IntegerType


object ClinicalService{

  /** Creates a DateTimeFormatter to standardize dates
   * ofPattern("format"): provides a readable format for the formatter
   *  parseDefaulting("ChronoField",value): replaces de missing field indicated in "ChronoField" by the value.
   */
  def dtf: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .appendOptional(DateTimeFormatter.ofPattern("dd-MM-yyyy"))
      .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      .appendOptional(DateTimeFormatter.ofPattern("dd-MM-yy"))
      .appendOptional(DateTimeFormatter.ofPattern("MM-yyyy"))
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter()

  /** Standardize date formats
   * @param date: date data
   * @return: correctly formatted date as (yyyy-MM-dd)
   */
  def dateFix (date: String): String ={LocalDate.parse(date, dtf).toString}
  def dateFixUDF = udf(dateFix(_))

  /**Replaces different genders from "male" and "female" by "unknown" according to gender specifications
   *param gender: gender string
   * @return: suitable gender according to gender specifications
   */
  def genderFix(gender : String) : String = {
    if (gender.matches("(f|F)(e|E)(m|M)(a|A)(l|L)(e|E)?")) { "female" }
    else if(gender.matches("(m|M)(a|A)(l|L)(e|E)?")){ "male" }
    else { "unknown" }
  }
  def genderFixUDF = udf(genderFix(_))

  /** Removes quotes " ” ”
   * @param id: any string
   * @return: same string but with no quotes on it
   */
  def quotesFix (id: String): String ={id.replaceAll("\"|”|”","")}
  def quotesFixUDF = udf(quotesFix(_))

  def main(args: Array[String]) {
    /** Set the log level to only print errors */
    Logger.getLogger("org").setLevel(Level.ERROR)

    /**The entry point into all functionality in Spark is the SparkSession class.
     * appName: gives a name to the application
     * master: tells the amount of core that can use for this script. In this case we want to use all the nodes so the way to indicate that is local[*]
     * getOrCreate: if there is an existing session it will take it, otherwise it will create a new one. This helps to not get dead sessions.
     */
    val spark = SparkSession
      .builder
      .appName("ClinicalServiceMapping")
      .master("local[*]")
      .getOrCreate()

    /**Load data from csv aggregating read options.
     * ("header", "true"): means the information comes with headers so first row will be for them.
     * ("delimiter", ","): means data is split by ,
     * csv("path"): tells the path where csv is at.
     */
    val records = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("data/records.csv")

    val savanaGender = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("data/original-gender-savana-gender.csv")

    val savanaService = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("data/original-service-savana-service.csv")

    /**Apply UDF to their respective column by using the method withColumn which creates or replaces a column with the one desired.
     *
     */
    val fixedRecords =
      records
        .withColumn("original_gender",genderFixUDF(col("original_gender")))
        .withColumn("original_document_id",quotesFixUDF(col("original_document_id")))
        .withColumn("original_document_date", dateFixUDF(col("original_document_date")))
        .withColumn("original_birthdate", dateFixUDF(col("original_birthdate")))

    /**Method join allows us to put together the tables involved, through the desired columns
     * @param: table to join, @param: columns related, @param: join type
     *In this case inner join is the most suitable to use.
     *At last we removed unnecessary columns added before by selecting only the ones we need.
     */
    val output = fixedRecords
      .join(savanaGender, fixedRecords("original_gender") === savanaGender("original_gender"), "inner")
      .join(savanaService, fixedRecords("original_service") === savanaService("original_service"), "inner")
      .select("original_document_date","original_patient_id","gender","original_birthdate","original_document_id","savana_service")

    /**Export our DataFrame to csv
     * ("delimiter", ","): writes "," between each data
     * mode("overwrite"): creates the file or overwrites it
     * csv("path"): desired path to store output csv
     */
    case class Clinical(document_date:Date, patient_id:BigInt, gender:Int, Birthdate:Date, document_id:BigInt, service:String)

    val outputFixed = output
      .withColumn("original_document_date",col("original_document_date").cast("Date"))
      .withColumn("original_patient_id",col("original_patient_id").cast("BigInt"))
      .withColumn("gender",col("gender").cast("int"))
      .withColumn("original_birthdate",col("original_birthdate").cast("Date"))
      .withColumn("original_document_id",col("original_document_id").cast("BigInt"))
      .withColumnRenamed("original_document_date","document_date")
      .withColumnRenamed("original_patient_id","patient_id")
      .withColumnRenamed("original_birthdate","Birthdate")
      .withColumnRenamed("original_document_id","document_id")
      .withColumnRenamed("savana_service","service")

    outputFixed
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .mode("overwrite")
      .csv("data/datacsv")

    spark.stop()
  }
}


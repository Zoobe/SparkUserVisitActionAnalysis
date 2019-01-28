import com.bigData.data.MockData
import org.apache.spark.sql.SparkSession

object mockTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    MockData.mock(spark)

    spark.stop()

  }
}

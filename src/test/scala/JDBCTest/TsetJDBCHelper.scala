package JDBCTest
import com.bigData.jdbc.JDBCHelper
object TsetJDBCHelper {
  def main(args: Array[String]): Unit = {
    val sql ="insert into spark_test values(?,?)"
    val param = Array[Any]("james",1)
    val t = JDBCHelper.getInstance
    t.executeUpdate(sql,param)
  }

}

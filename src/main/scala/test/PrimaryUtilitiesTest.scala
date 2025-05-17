package test

import com.wts.kayan.app.utility.PrimaryUtilities
import javafx.beans.binding.Bindings.when
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import java.util.Date

class PrimaryUtilitiesTest  extends AnyFunSuite with MockitoSugar{

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("PrimaryUtilitiesTest")
    .master("local[1]")
    .getOrCreate()

  test("getMaxPartition should return the most recent partition") {
    val fsMock = mock[FileSystem]
    val pathMock = new Path("/data/")
    val fileStatuses = Array(
      createFileStatus("/data/date=2024-12-01"),
      createFileStatus("/data/date=2024-12-05")
    )
    when(fsMock.listStatus(pathMock)).thenReturn(fileStatuses)))

    val result = PrimaryUtilities.getMaxPartition("/data/")(spark)
    assert(result == "2024-12-05")



  }

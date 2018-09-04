import java.sql.Date
import java.text.SimpleDateFormat

import com.datastax.spark.connector._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.util.control.NonFatal
import scala.util.{Either, Left, Right, Try}

case class Account(number: String, firstName: String, lastName: String)

case class Transaction(id: Long, account: Account, date: Date, amount: Double,
                       description: String)

case class TransactionForAverage(accountNumber: String, amount: Double, description: String,
                                 date: Date)

case class SimpleTransaction(id: Long, account_number: String, amount: Double,
                             date: Date, description: String)

case class UnparsableTransaction(id: Option[Long], originalMessage: String, exception: Throwable)

case class AggregateData(totalSpending: Double, numTx: Int, windowSpendingAvg: Double) {
  val averageTx: Double = if (numTx > 0) totalSpending / numTx else 0
}

case class EvaluatedSimpleTransaction(tx: SimpleTransaction, isPossibleFraud: Boolean)

object Detector {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local[3]")
      .appName("Fraud Detector")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .enableHiveSupport // should not be needed in spark 2
      .getOrCreate()

    import spark.implicits._

    val financesDS: Dataset[Transaction] = spark.read.json("Data/finances-small.json")
      .withColumn(colName = "date",
        col = to_date(unix_timestamp($"Date", "MM/dd/yyyy").cast("timestamp")))
      .as[Transaction]

    val accountNumberPrevious4WindowSpec: WindowSpec = Window.partitionBy($"AccountNumber")
      .orderBy($"Date").rowsBetween(-4, Window.currentRow)
    val rollingAvgForPrevious4PerAccount: Column = avg($"Amount").over(accountNumberPrevious4WindowSpec)

    financesDS
      .na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date"))
      .na.fill("Unknown", Seq("Description")).as[Transaction]
      .where($"Amount" =!= 0 || $"Description" === "Unknown")
      .select(
        $"Account.Number".as("AccountNumber").as[String],
        $"Amount".as[Double],
        $"Date".as[Date],
        $"Description".as[String])
      .withColumn("RollingAverage", rollingAvgForPrevious4PerAccount)
      .write.mode(SaveMode.Overwrite)
      .parquet("Output/finances-small")

    if (financesDS.hasColumn("_corrupt_record")) {
      financesDS.where($"_corrupt_record".isNotNull)
        .select($"_corrupt_record")
        .write.mode(SaveMode.Overwrite)
        .text("Output/corrupt_finances")
    }

    financesDS
      .map(tx => (s"${tx.account.firstName} ${tx.account.lastName}", tx.account.number))
      .distinct
      .toDF("FullName", "AccountNumber")
      .coalesce(5)
      .write.mode(SaveMode.Overwrite).json("Output/finances-small-accounts")

    financesDS
      .select($"account.number".as("accountNumber").as[String],
        $"amount".as[Double],
        $"description".as[String],
        $"date".as[Date])
      .as[TransactionForAverage]
      .groupBy($"AccountNumber")
      .agg(avg($"Amount").as("average_transaction"),
        sum($"Amount").as("total_transactions"),
        count($"Amount").as("number_of_transactions"),
        max($"Amount").as("max_transaction"),
        min($"Amount").as("min_transaction"),
        stddev($"Amount").as("standard_deviation_amount"),
        collect_set($"Description").as("unique_transaction_descriptions"))
      .withColumnRenamed("accountNumber", "account_number")
      .coalesce(5)
      .write
      .mode(SaveMode.Overwrite)
      .cassandraFormat("account_aggregates", "finances")
      .save

    val checkpointDir = "file:///checkpoint"
    val streamingContext: StreamingContext = StreamingContext.getOrCreate(checkpointDir, () => createStreamingContext(checkpointDir, spark))

    streamingContext.start
    streamingContext.awaitTermination
  }

  private def createStreamingContext(checkpointDir: String, spark: SparkSession): StreamingContext = {
    val ssc = new StreamingContext(spark.sparkContext, batchDuration = Seconds(5))
    ssc.checkpoint(checkpointDir)
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("transactions"),
        Map("metadata.broker.list" -> "localhost:9092", "group.id" -> "transactions-group"))
    )
    kafkaStream.map((keyVal: ConsumerRecord[String, String]) => tryConversionToSimpleTransaction(keyVal.value()))
      .flatMap(_.right.toOption)
      .map(simpleTx => (simpleTx.account_number, simpleTx))
      .mapValues(tx => List(tx))
      .reduceByKeyAndWindow(
        reduceFunc = (txs, otherTxs) => txs ++ otherTxs,
        invReduceFunc = (txs, oldTxs) => txs diff oldTxs,
        windowDuration = Seconds(10),
        slideDuration = Seconds(10))
      .mapWithState(
        StateSpec.function((_: String,
                            newTxsOpt: Option[List[SimpleTransaction]],
                            aggData: State[AggregateData]) => {
          val newTxs: List[SimpleTransaction] = newTxsOpt.getOrElse(List.empty[SimpleTransaction])
          val calculatedAggTuple: (Double, Int) = newTxs.foldLeft((0.0, 0))((currAgg, tx) => (currAgg._1 + tx.amount, currAgg._2 + 1))
          val calculatedAgg = AggregateData(calculatedAggTuple._1, calculatedAggTuple._2,
            if (calculatedAggTuple._2 > 0) calculatedAggTuple._1 / calculatedAggTuple._2 else 0)
          val oldAggDataOption: Option[AggregateData] = aggData.getOption
          aggData.update(
            oldAggDataOption match {
              case Some(oldAggData) =>
                AggregateData(oldAggData.totalSpending + calculatedAgg.totalSpending,
                  oldAggData.numTx + calculatedAgg.numTx, calculatedAgg.windowSpendingAvg)
              case None => calculatedAgg
            }
          )
          newTxs.map(tx => EvaluatedSimpleTransaction(tx, tx.amount > 4000))
        }))
      .stateSnapshots
      .transform(dStreamRDD => {
        val sc: SparkContext = dStreamRDD.sparkContext
        val historicAggsOption: Option[RDD[_]] = sc.getPersistentRDDs
          .values.find(_.name == "storedHistoric")
        val historicAggs: RDD[(String, Double)] = historicAggsOption match {
          case Some(persistedHistoricAggs) =>
            persistedHistoricAggs.asInstanceOf[RDD[(String, Double)]]
          case None =>
            val retrievedHistoricAggs: RDD[(String, Double)] = sc.cassandraTable("finances", "account_aggregates")
              .select("account_number", "average_transaction")
              .map(cassandraRow => (
                cassandraRow.get[String]("account_number"),
                cassandraRow.get[Double]("average_transaction")))
            retrievedHistoricAggs.setName("storedHistoric")
            retrievedHistoricAggs.cache
        }
        dStreamRDD.join(historicAggs)
      })
      .filter { case (_, (aggData, historicAvg)) =>
        aggData.averageTx - historicAvg > 2000 || aggData.windowSpendingAvg - historicAvg > 2000
      }
      .print
    ssc
  }

  def tryConversionToSimpleTransaction(logLine: String): Either[UnparsableTransaction, SimpleTransaction] = {
    logLine.split(',') match {
      case Array(id, date, acctNum, amt, desc) =>
        var parsedId: Option[Long] = None
        try {
          parsedId = Some(id.toLong)
          Right(SimpleTransaction(parsedId.get, acctNum, amt.toDouble,
            new java.sql.Date(new SimpleDateFormat("MM/dd/yyyy").parse(date).getTime), desc))
        } catch {
          case NonFatal(exception) => Left(UnparsableTransaction(parsedId, logLine, exception))
        }
      case _ => Left(UnparsableTransaction(None, logLine,
        new Exception("Log split on comma does not result in a 5 element array.")))
    }
  }

  implicit class DatasetHelper[T](ds: Dataset[T]) {
    def hasColumn(columnName: String): Boolean = Try(ds(columnName)).isSuccess
  }

}
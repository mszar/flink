package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object MultipleStreams {

  def demoUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEventsKafka: DataStream[ShoppingCartEvent] = env
      .addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))

    val shoppingCartEventsFiles: DataStream[ShoppingCartEvent] = env
      .addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("files")))

    val combinedShoppingCartEventsStream: DataStream[ShoppingCartEvent] =
      shoppingCartEventsKafka.union(shoppingCartEventsFiles)

    combinedShoppingCartEventsStream.print()

    env.execute()
  }

  def demoWindowJoins(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEventsKafka: DataStream[ShoppingCartEvent] = env
      .addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("kafka")))

    val catalogEvents = env.addSource(new CatalogEventsGenerator(2000))

    val joinedStreams = shoppingCartEventsKafka
      .join(catalogEvents)
      .where(shoppingCartEventsKafka => shoppingCartEventsKafka.userId)
      .equalTo(catalogEvents => catalogEvents.userId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply{
        ((shoppingCartEventsKafka, catalogEvents) =>
          s"User ${shoppingCartEventsKafka.userId} browsed at ${catalogEvents.time} and bought at ${shoppingCartEventsKafka.time}")
      }

    joinedStreams.print()
    env.execute()
  }

  def demoIntervalJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEventsKafka = env
      .addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("kafka")))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )
      .keyBy(_.userId)


    val catalogEvents = env.addSource(new CatalogEventsGenerator(2000))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[CatalogEvent] {
            override def extractTimestamp(element: CatalogEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )
      .keyBy(_.userId)

    val intervalJoinedStream = shoppingCartEventsKafka
      .intervalJoin(catalogEvents)
      .between(Time.seconds(-2), Time.seconds(2))
      .process(new ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String] {
        override def processElement(
                                     left: ShoppingCartEvent,
                                     right: CatalogEvent,
                                     ctx: ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String]#Context,
                                     out: Collector[String]) =
          out.collect(s"${left.userId}, browsed at ${right.time} and bought at ${left.time}")
      })

    intervalJoinedStream.print()
    env.execute()

  }

  def main(args: Array[String]): Unit = {
    demoIntervalJoin()

  }
}

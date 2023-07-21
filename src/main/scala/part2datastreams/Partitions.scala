package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object Partitions {

  def demoPartitioner(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents: DataStream[ShoppingCartEvent] = env
      .addSource(new SingleShoppingCartEventsGenerator(100))

    val partitioner: Partitioner[String] = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        println(s"Number of max partitions: $numPartitions")
        key.hashCode % numPartitions
      }
    }

    val partitionedStream = shoppingCartEvents.partitionCustom(
      partitioner,
      event => event.userId
    )

    partitionedStream.print()
    env.execute()


  }


  def main(args: Array[String]): Unit = {
    demoPartitioner()
  }

}

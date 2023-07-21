package part2datastreams

import generators.shopping._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Triggers {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


  def demoCountTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] =  env
      .addSource(function = new ShoppingCartEventsGenerator(500, 2))
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .trigger(CountTrigger.of[TimeWindow](5))
      .process(new CountByWindowAll)

    shoppingCartEvents.print()
    env.execute()

  }

  def demoPurgingTrigger(): Unit = {
    val shoppingCartEvents: DataStream[String] = env
      .addSource(function = new ShoppingCartEventsGenerator(500, 2))
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](5)))
      .process(new CountByWindowAll)

    shoppingCartEvents.print()
    env.execute()

  }

  def main(args: Array[String]): Unit = {
    demoPurgingTrigger()
  }
  
  class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
    }
  }
}



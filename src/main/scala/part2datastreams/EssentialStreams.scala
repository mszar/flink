package part2datastreams

import org.apache.flink.streaming.api.scala._

object EssentialStreams {

  def applicationTemplate(): Unit = {
      // 1 - execution environment
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

      // in between, add any sort of computations
      import org.apache.flink.streaming.api.scala._ //import TypeInformation for the data of DataStreams
      val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

      simpleNumberStream.print()

      // at the end
      env.execute() // trigger all the computations that were DESCRIBED earlier
  }

  def  demoTransformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)
    println(s"Current parallelism: ${env.getParallelism}")
    env.setParallelism(2)
    println(s"Current parallelism: ${env.getParallelism}")


    // map
    val doubledNumbers: DataStream[Int] = numbers.map(_ * 2)

    // flatMap
    val expandedNumbers: DataStream[Int] = numbers.flatMap(n => List(n, n+1))

    // filter
    val filteredNumbers: DataStream[Int] = numbers
      .filter(_  % 2 == 0)
      .setParallelism(4)

    val finalData = expandedNumbers.writeAsText("output/expandedStream.txt")
    finalData.setParallelism(1)

    env.execute()
  }

  case class FizzBuzzResult(n: Long, output: String)

  def fizzBuzzExercise(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //val numbers: DataStream[Int] = env.fromElements((1 to 100): _*)
    val numbers = env.fromSequence(1, 100)

    val fizzbuzz = numbers
      .map { n=>
        val output =
            if (n % 3 == 0 && n % 5 == 0) "fizzbuzz"
            else if (n % 3 == 0) "fizz"
            else if (n % 5 == 0) "buzz"
            else s"$n"
      FizzBuzzResult(n, output)
    }
      .filter(_.output == "fizzbuzz") // DataStream[FizzBuzzResult]
      .map(_.n) // DataStream[Long]

    fizzbuzz.writeAsText("output/fizzbuzz.txt").setParallelism(1)
    env.execute()

  }

  def fizzBuzzExercise2(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //val numbers: DataStream[Int] = env.fromElements((1 to 100): _*)
    val numbers = env.fromSequence(1, 100)

    val fizzbuzz = numbers
      .map[FizzBuzzResult] {(x: Long) => x match {
        case n if n % 3 == 0 && n % 5 == 0 => FizzBuzzResult(n, "fizzbuzz")
        case n if n % 3 == 0 => FizzBuzzResult(n, "fizz")
        case n if n % 5 == 0 => FizzBuzzResult(n, "buzz")
        case n => FizzBuzzResult(n, s"$n")
      }}


    fizzbuzz.writeAsText("output/fizzbuzz.txt").setParallelism(1)
    env.execute()

  }

  def main(args: Array[String]): Unit = {
    fizzBuzzExercise2()
  }

}

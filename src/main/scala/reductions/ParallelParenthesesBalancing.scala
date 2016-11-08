package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    chars.foldLeft(0)((acc, char) => char match {
      case '(' => if (acc < 0) -1 else acc + 1
      case ')' => if (acc <= 0) -1 else acc - 1
      case _ => acc
    }) == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, left: Int, right: Int) : (Int, Int) = {
      (idx until until).foldLeft((0, 0))((acc, current) => current match {
        case ')' => (acc._1-1, acc._2+1)
        case '(' => (acc._1+1, acc._2-1)
        case _ => acc
      })
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      val size = until - from
      if (size < threshold) traverse(from, until, 0, 0)
      else {
          val m = size / 2
          val ( (l1, r1), (l2, r2)) = parallel(reduce(from, from + m), reduce(from + m, until))
          (l1+r1, l2+r2)
      }

    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}

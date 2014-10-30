package akka.stream.scaladsl

import scala.util.control.NoStackTrace
import FlowGraphImplicits._
import akka.stream.FlowMaterializer
import akka.stream.testkit.AkkaSpec
import akka.stream.testkit.StreamTestKit.AutoPublisher
import akka.stream.testkit.StreamTestKit.OnNext
import akka.stream.testkit.StreamTestKit.PublisherProbe
import akka.stream.testkit.StreamTestKit.SubscriberProbe

object GraphRouteSpec {

  /**
   * This is fair in that sense that after dequeueing from an input it yields to other inputs if
   * they are available. Or in other words, if all inputs have elements available at the same
   * time then in finite steps all those elements are dequeued from them.
   */
  class Fair[T] extends Route[T]("fairRoute") {
    import Route._
    val out1 = createOutputPort[T]()
    val out2 = createOutputPort[T]()

    override def createRouteLogic: RouteLogic[T] = new RouteLogic[T] {
      override def waitForAllDownstreams: Boolean = true
      override def outputHandles(outputCount: Int) = Vector(out1, out2)
      override def initialState = State[T](DemandFromAny(out1, out1)) { (ctx, preferredOutput, element) ⇒
        ctx.emit(preferredOutput, element)
        SameState
      }
    }
  }

  /**
   * It never skips an input while cycling but waits on it instead (closed inputs are skipped though).
   * The fair merge above is a non-strict round-robin (skips currently unavailable inputs).
   */
  class StrictRoundRobin[T] extends Route[T]("roundRobinRoute") {
    import Route._
    val out1 = createOutputPort[T]()
    val out2 = createOutputPort[T]()

    override def createRouteLogic = new RouteLogic[T] {

      override def waitForAllDownstreams: Boolean = true

      override def outputHandles(outputCount: Int) = Vector(out1, out2)

      val toOutput1: State[T] = State[T](DemandFrom(out1)) { (ctx, _, element) ⇒
        ctx.emit(out1, element)
        toOutput2
      }

      val toOutput2 = State[T](DemandFrom(out2)) { (ctx, _, element) ⇒
        ctx.emit(out2, element)
        toOutput1
      }

      override def initialState = toOutput1
    }
  }

  class Unzip[A, B] extends Route[(A, B)]("unzip") {
    import Route._
    val outA = createOutputPort[A]()
    val outB = createOutputPort[B]()

    override def createRouteLogic() = new RouteLogic[(A, B)] {
      var lastInA: Option[A] = None
      var lastInB: Option[B] = None

      override def waitForAllDownstreams: Boolean = true

      override def outputHandles(outputCount: Int) = {
        require(outputCount == 2, s"Unzip must have two connected outputs, was $outputCount")
        Vector(outA, outB)
      }

      override def initialState = State[Any](DemandFromAll(outA, outB)) { (ctx, _, element) ⇒
        val (a, b) = element
        ctx.emit(outA, a)
        ctx.emit(outB, b)
        SameState
      }

      // FIXME if one leg cancels it should stop
      //      override def initialCompletionHandling = eagerClose
    }
  }

}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class GraphRouteSpec extends AkkaSpec {
  import GraphRouteSpec._

  implicit val materializer = FlowMaterializer()

  val in = Source(List("a", "b", "c", "d", "e"))

  val out1 = Sink.publisher[String]
  val out2 = Sink.publisher[String]

  "Route" must {

    "build simple fair route" in {
      val m = FlowGraph { implicit b ⇒
        val route = new Fair[String]
        in ~> route.in
        route.out1 ~> out1
        route.out2 ~> out2
      }.run()

      val s1 = SubscriberProbe[String]
      val p1 = m.get(out1)
      p1.subscribe(s1)
      val sub1 = s1.expectSubscription()
      val s2 = SubscriberProbe[String]
      val p2 = m.get(out2)
      p2.subscribe(s2)
      val sub2 = s2.expectSubscription()

      sub1.request(1)
      sub2.request(1)

      val elements1 = s1.probe.receiveWhile() {
        case OnNext(elem) ⇒
          sub1.request(1)
          elem
      }
      val elements2 = s2.probe.receiveWhile() {
        case OnNext(elem) ⇒
          sub2.request(1)
          elem
      }

      println(s"# elements1: $elements1") // FIXME
      println(s"# elements2: $elements2") // FIXME

      (elements1 ++ elements2).toSet should be(Set("a", "b", "c", "d", "e"))
      (elements1.toSet.intersect(elements2.toSet)) should be(Set.empty)

      s1.expectComplete()
      s2.expectComplete()
    }

    "build simple round-robin route" in {
      val m = FlowGraph { implicit b ⇒
        val route = new StrictRoundRobin[String]
        in ~> route.in
        route.out1 ~> out1
        route.out2 ~> out2
      }.run()

      val s1 = SubscriberProbe[String]
      val p1 = m.get(out1)
      p1.subscribe(s1)
      val sub1 = s1.expectSubscription()
      val s2 = SubscriberProbe[String]
      val p2 = m.get(out2)
      p2.subscribe(s2)
      val sub2 = s2.expectSubscription()

      sub1.request(10)
      sub2.request(10)

      s1.expectNext("a")
      s2.expectNext("b")
      s1.expectNext("c")
      s2.expectNext("d")
      s1.expectNext("e")

      s1.expectComplete()
      s2.expectComplete()
    }

    "build simple unzip route" in {
      val outA = Sink.publisher[Int]
      val outB = Sink.publisher[String]

      val m = FlowGraph { implicit b ⇒
        val route = new Unzip[Int, String]
        Source(List(1 -> "A", 2 -> "B", 3 -> "C")) ~> route.in
        route.outA ~> outA
        route.outB ~> outB
      }.run()

      val s1 = SubscriberProbe[Int]
      val p1 = m.get(outA)
      p1.subscribe(s1)
      val sub1 = s1.expectSubscription()
      val s2 = SubscriberProbe[String]
      val p2 = m.get(outB)
      p2.subscribe(s2)
      val sub2 = s2.expectSubscription()

      sub1.request(10)
      sub2.request(10)

      s1.expectNext(1)
      s2.expectNext("A")
      s1.expectNext(2)
      s2.expectNext("B")
      s1.expectNext(3)
      s2.expectNext("C")

      s1.expectComplete()
      s2.expectComplete()
    }

  }
}


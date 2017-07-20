import akka.stream._
import akka.stream.scaladsl._

import akka.{ NotUsed, Done }
import akka.actor.ActorSystem
import akka.util.ByteString
import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths

// This application is a simple walkthrough of the lessons outlined in the
// akka streams quickstart webpage:
//    http://doc.akka.io/docs/akka/current/scala/stream/stream-quickstart.html
object Main extends App {
  
  val simpleStreamSource = simpleStream()
  // streamToFile(simpleStreamSource)
  // complexStream()


  // A very simple stream consisting entirely of a Source which
  // generates a range of integers
  def simpleStream() = {

    // Streams are run by an actor system so we need to provide 
    // the implict actor system and a materializer used by the 
    // akka streams `run` functions (such as `source.runForeach` below)
    implicit val system = ActorSystem("QuickStart")
    implicit val materializer = ActorMaterializer()

    // A simple source that generates a series of integers
    val source: Source[Int, NotUsed] = Source(1 to 10)

    // Stream `run` functions return a Future[Done] object which
    // resolves when the stream finishes.  This Done is used to 
    // tell the actor system running the stream to terminate.
    val done: Future[Done] = source.runForeach(i => println(i))(materializer)

    // We'll need an execution context; we can use the actor system dispatcher
    implicit val ec = system.dispatcher

    // Kill the actor system when the stream finishes
    done.onComplete(_ => {
        system.terminate()
        println("...completed simpleStream()\n")
    })

    // That concludes this simple demonstration of a simple stream.  I'm only 
    // returning our source to demonstrait our next point in `streamToFile()`
    source
  }

  /** This function demonstraits that a Source is "just a description 
   *  of what you want to run" and can be reused in different ways. 
   *  In this function, we use the source returned from `simpleStream()`
   *  as the source of input for a text file we're writing to disk.
   */
  def streamToFile(source: Source[Int,NotUsed]) = {
    implicit val system = ActorSystem("QuickStart")
    implicit val materializer = ActorMaterializer()
    implicit val ec = system.dispatcher
    val filename = "factorials.txt"

    // Using the source that generated the numeric range printout in 
    // `simpleStream()`, we produce a new Source of factorials based
    // on the int source
    val factorials: Source[BigInt,NotUsed] = source.scan(BigInt(1))((acc, next) => acc * next)

    // This time we write the factorial results to disk
    val result: Future[IOResult] =
      factorials
        .map(num => ByteString(s"$num\n"))
        .runWith(FileIO.toPath(Paths.get(filename)))

    // When the file is written print message to command line and 
    // terminate the actor system running our stream
    result.onSuccess {
      case io if (io.wasSuccessful) => {
        println(s"source > ${filename}") 
        system.terminate()
        println("...completed streamToFile()") 
      }
    }
  }

  /**
   * An example of a complex stream that composes a closed graph. It's all 
   * framework and no logic, so if you call this function it just runs endlessly
   * and silently, but it is a good illustration of how multiple elements 
   * are assembled into a complex stream.  
   * See: http://doc.akka.io/docs/akka/current/scala/stream/stream-composition.html#composing-complex-systems
   */
  def complexStream() = {
    import GraphDSL.Implicits._

    // A graph is built up out of flows, sinks and sources.  If all we had was 
    // those three elements, then our stream would always be a straight line since
    // each of those has a maximum of one input or output.  Graphs allow us to 
    // construct a stream where logic can fan out and take different paths, or 
    // fan in and be collected together again.
    // See: http://doc.akka.io/docs/akka/current/scala/stream/stream-graphs.html#working-with-graphs
    val myGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      val A: Outlet[Int]                  = builder.add(Source.single(0)).out
      val B: UniformFanOutShape[Int, Int] = builder.add(Broadcast[Int](2))
      val C: UniformFanInShape[Int, Int]  = builder.add(Merge[Int](2))
      val D: FlowShape[Int, Int]          = builder.add(Flow[Int].map(_ + 1))
      val E: UniformFanOutShape[Int, Int] = builder.add(Balance[Int](2))
      val F: UniformFanInShape[Int, Int]  = builder.add(Merge[Int](2))
      val G: Inlet[Any]                   = builder.add(Sink.foreach(println)).in

      // Some things to notice:
      // - A is the only source of data
      // - B and E fan out to C & D and F & G (respectively)
      // - C and F fan in from B & F and E & C (respectively)
      // - G is the only element that doesn't pass data on (a Sink)
      // 
      // Also, it is very cool that the code for the graph can be represented
      // graphically like this. But you shoul know that there are other ways 
      // to construct the graph (in fact, this syntax may be less common).

                    C     <~      F
      A  ~>  B  ~>  C     ~>      F
             B  ~>  D  ~>  E  ~>  F
                           E  ~>  G

      // Taken as a whole, this graph has no input and no output and is 
      // considered a "closed shape".  A graph that did not specify a sink
      // could be hooked up to another graph and effectively used as a source, 
      // but that's not the case here.  Closed shapes are islands unto themselves
      // and can be used in isolation.
      ClosedShape
    })

    // Because myGraph is a closed shape, we "use it directly".  Here, we 
    // take myGraph and build an actor system that executes this closed graph.
    // Doing so is anaologous to the actor system that the `simpleStream()` 
    // function used when it called `source.runForeach()`
    implicit val system = ActorSystem("QuickStart")
    implicit val materializer = ActorMaterializer()

    val runnableGraphJob = RunnableGraph.fromGraph(myGraph)
    runnableGraphJob.run()
  } 
}

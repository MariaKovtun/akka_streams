import akka.NotUsed
import akka.actor.typed.{ActorSystem, Behavior, Props}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.stream.alpakka.slick.scaladsl.{Slick, SlickSession}
import akka.stream.scaladsl.{Flow, GraphDSL, Sink, Source, Broadcast, RunnableGraph}
import akka.stream.{ClosedShape, Graph}
import akka_typed.TypedCalculatorWriteSide._
import scalikejdbc.DB.using
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB}
import slick.jdbc.PostgresProfile.api._
import akka_typed.CalculatorRepository.{Result,getLatestsOffsetAndResult, initDatabase, updatedResultAndOffset}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}

object  akka_typed{

  trait CborSerialization

  val persId = PersistenceId.ofUniqueId("001")

  object TypedCalculatorWriteSide{
    sealed trait Command
    case class Add(amount: Int) extends Command
    case class Multiply(amount: Int) extends Command
    case class Divide(amount: Int) extends Command

    sealed trait Event
    case class Added(id:Int, amount: Int) extends Event
    case class Multiplied(id:Int, amount: Int) extends Event
    case class Divided(id:Int, amount: Int) extends Event

    final case class State(value:Int) extends CborSerialization
    {
      def add(amount: Int): State = copy(value = value + amount)
      def multiply(amount: Int): State = copy(value = value * amount)
      def divide(amount: Int): State = copy(value = value / amount)
    }

    object State{
      val empty = State(0)
    }


    def handleCommand(
                       persistenceId: String,
                       state: State,
                       command: Command,
                       ctx: ActorContext[Command]
                     ): Effect[Event, State] =
      command match {
        case Add(amount) =>
          ctx.log.info(s"receive adding  for number: $amount and state is ${state.value}")
          val added = Added(persistenceId.toInt, amount)
          Effect
            .persist(added)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Multiply(amount) =>
          ctx.log.info(s"receive multiplying  for number: $amount and state is ${state.value}")
          val multiplied = Multiplied(persistenceId.toInt, amount)
          Effect
            .persist(multiplied)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
        case Divide(amount) =>
          ctx.log.info(s"receive dividing  for number: $amount and state is ${state.value}")
          val divided = Divided(persistenceId.toInt, amount)
          Effect
            .persist(divided)
            .thenRun{
              x=> ctx.log.info(s"The state result is ${x.value}")
            }
      }

    def handleEvent(state: State, event: Event, ctx: ActorContext[Command]): State =
      event match {
        case Added(_, amount) =>
          ctx.log.info(s"Handling event Added is: $amount and state is ${state.value}")
          state.add(amount)
        case Multiplied(_, amount) =>
          ctx.log.info(s"Handling event Multiplied is: $amount and state is ${state.value}")
          state.multiply(amount)
        case Divided(_, amount) =>
          ctx.log.info(s"Handling event Divided is: $amount and state is ${state.value}")
          state.divide(amount)
      }

    def apply(): Behavior[Command] =
      Behaviors.setup{ ctx =>
        EventSourcedBehavior[Command, Event, State](
          persistenceId = persId,
          State.empty,
          (state, command) => handleCommand("001", state, command, ctx),
          (state, event) => handleEvent(state, event, ctx)
        )
      }

  }


  case class TypedCalculatorReadSide(system: ActorSystem[NotUsed])(implicit executionContext: ExecutionContextExecutor) {
    implicit val materializer = system.classicSystem
    implicit val session: SlickSession = SlickSession.forConfig("slick-postgres")
    var res: Result = getLatestsOffsetAndResultSlick
    val startOffset: Long = if (offset == 1) 1 else offset + 1

    val readJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

    val source: Source[EventEnvelope, NotUsed] = readJournal.eventsByPersistenceId("001", startOffset, Long.MaxValue)

    def updateState(event: EventEnvelope): Result =
      event.event match {
        case Added(_, amount) =>
          val newRes = Result(res.state + amount, event.sequenceNr)
          updatedResultAndOffset(newRes)
          println(s"Log from Added: ${newRes.state}")
          newRes
        case Multiplied(_, amount) =>
          val newRes = Result(res.state * amount, event.sequenceNr)
          updatedResultAndOffset(newRes)
          println(s"Log from Multiplied:  ${newRes.state}")
          newRes
        case Divided(_, amount) =>
          val newRes = Result(res.state / amount, event.sequenceNr)
          updatedResultAndOffset(newRes)
          println(s"Log from Divided:  ${newRes.state}")
          newRes
      }

    val graph: Graph[ClosedShape.type, NotUsed] = GraphDSL.create() {
      implicit builder: GraphDSL.Builder[NotUsed] =>
        val input = builder.add(source)
        val stateUpdater = builder.add(Flow[EventEnvelope].map(e => updateState(e)))
        val localSaveOutput = builder.add(Sink.foreach[Result] { r =>
          res = res.copy(state = r.state)
          println("something to print")
        })

        val dbSaveOutput = builder.add(
          Slick.sink(updatedResultAndOffsetSlick)
        )
        val broadcast = builder.add(Broadcast[Result](2))

        import GraphDSL.Implicits._
        input ~> stateUpdater ~> broadcast

        broadcast.out(0) ~> dbSaveOutput
        broadcast.out(1) ~> localSaveOutput

        ClosedShape
    }
  }

    object CalculatorRepository {

      case class Result(state: Double, offset: Long)

      def getLatestsOffsetAndResultSlick(implicit
                                         executionContext: ExecutionContextExecutor,
                                         session: SlickSession
                                        ): Result = {
        val q = sql"""select calculated_value, write_side_offset from public.result where id = 1;"""
          .as[(Double, Long)]
          .headOption

        val f = session.db.run(q).map(v => v.flatMap(r => Some(Result(r._1, r._2))))
        Await.result(f, 10000.nanos)
      }.getOrElse(throw new RuntimeException("no values in db"))

      def getLatestOffsetAndResult: (Int, Double) = {
        val entities =
          DB readOnly { session =>
            session.list("select * from public.result where id = 1;") { row =>
              (row.int("write_side_offset"), row.double("calculated_value"))
            }
          }
        entities.head
      }

      //homework how to do
      def updatedResultAndOffset(result: Result): Unit = {
        using(DB(ConnectionPool.borrow())) { db =>
          db.autoClose(true)
          db.localTx {
            _.update(
              "update public.result set calculated_value = ?, write_side_offset = ? where id = 1",
              result.state,
              result.offset
            )
          }
        }
      }
    }

    private def updatedResultAndOffsetSlick =
      (res: Result) =>
        sqlu"update public.result set calculated_value = ${res.state}, write_side_offset = ${res.offset} where id = 1"

    def apply(): Behavior[NotUsed] =
      Behaviors.setup { ctx =>
        val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeAcorRef ! Add(10)
        writeAcorRef ! Multiply(2)
        writeAcorRef ! Divide(5)
        Behaviors.same
      }
    def execute(command: Command): Behavior[NotUsed] =
      Behaviors.setup { ctx =>
        val writeAcorRef = ctx.spawn(TypedCalculatorWriteSide(), "Calc", Props.empty)
        writeAcorRef ! command
        Behaviors.same
      }
    def main(args: Array[String]): Unit = {
      val value                                               = akka_typed()
      implicit val system: ActorSystem[NotUsed]               = ActorSystem(value, "akka_typed")
      implicit val executionContext: ExecutionContextExecutor = system.executionContext

      val program = TypedCalculatorReadSide(system).graph
      RunnableGraph.fromGraph(program).run()
    }

  }
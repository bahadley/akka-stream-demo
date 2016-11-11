package rcb.tpcdi 

import akka.actor.{ActorLogging, Props}
import akka.stream.actor.{ActorSubscriber, WatermarkRequestStrategy}
import akka.stream.actor.ActorSubscriberMessage._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import slick.driver.H2Driver.api._


class Ingest extends ActorSubscriber with ActorLogging {
  import context.dispatcher

  override val requestStrategy = WatermarkRequestStrategy(50)

  implicit val timeout = Timeout(2 seconds)

  var db = None : Option[Database]

  val cb =
    new CircuitBreaker(
      context.system.scheduler,
      maxFailures = 1,
      callTimeout = 1.second,
      resetTimeout = 1.minute).onOpen(notifyMeOnOpen())

  override def preStart(): Unit = {
    db = Some(Database.forConfig("tpcdi", ConfigFactory.load()))
    super.preStart()
  }

  override def postStop(): Unit = {
    db match {
      case None => throw new NullPointerException(ERR_MSG_DB)
      case Some(db) => db.close 
    }
    super.postStop()
  }

  def receive = {
    case OnNext(tr: TaxRate) =>
      db match {
        case None => 
          throw new NullPointerException(ERR_MSG_DB) 
        case Some(db) => 
          Await.result(cb.guard(db.run(insertTx(tr))), timeout.duration)
          //cb.withCircuitBreaker(db.run(insertTx(tr))) onFailure { 
          //  case _ => log.error("Persistence failed")
          //}
      }
    case OnError(err: Exception) => 
      log.error(err, "Receieved exception in stream")
      context.stop(self)
    case OnComplete => 
      log.info("Stream completed!")
      context.stop(self)
    case _ =>
  }

  def insertTx(tx: TaxRate): DBIO[Int] =
    sqlu"""insert into taxrate (tx_id, tx_name, tx_rate)
      values (${tx.tx_id}, ${tx.tx_name}, ${tx.tx_rate})"""

  def notifyMeOnOpen(): Unit =
    log.warning("CircuitBreaker is now open, and will not close for one minute")

  val ERR_MSG_DB = "Connection pool is uninitialized" 
}

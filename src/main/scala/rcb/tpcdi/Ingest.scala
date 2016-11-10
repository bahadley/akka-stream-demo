package rcb.tpcdi 

import akka.actor.{Actor, ActorLogging}
import akka.stream.actor.ActorSubscriber
import scala.concurrent.duration._
import scala.language.postfixOps
import slick.driver.H2Driver.api._

class Ingest extends ActorSubscriber with ActorLogging {
  import context.dispatcher

  val requestStrategy = WatermarkRequestStrategy(50)

  var db = None : Option[Database]

  val ocb = new Ocb(
    scheduler = context.system.scheduler, 
    maxFailures = 5, 
    callTimeout = 2, 
    resetTimeout = 1)

  override def preStart(): Unit = {
    db = Some(Database.forConfig("tpcdi", context.system.settings.config))
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

    case OnNext(tr: CreateTaxRate) =>
      db match {
        case None => 
          throw new NullPointerException(ERR_MSG_DB) 
        case Some(db) => 
          ocb.guard(db.run(insertTx(msg))) onFailure {
            case _ => log.error("Creation failed")
          }
      }

    case OnError(err: Exception) => 
      log.error(err, "Receieved exception in stream")
      context.stop(self)

    case OnComplete => 
      log.info("Stream completed!")
      context.stop(self)

    case _ =>

  }

  def insertTx(tx: CreateTaxRate): DBIO[Int] =
    sqlu"""insert into taxrate (tx_id, tx_name, tx_rate)
      values (${tx.tx_id}, ${tx.tx_name}, ${tx.tx_rate})"""

  val ERR_MSG_DB = "Connection pool is uninitialized" 
}

package rcb.tpcdi 

import akka.actor.{Actor, ActorLogging}
import akka.pattern.pipe
import com.typesafe.config.ConfigFactory
import slick.driver.H2Driver.api._
import scala.concurrent.duration._
import scala.language.postfixOps


class DataAccess extends Actor with ActorLogging {
  import context.dispatcher

  var db = None : Option[Database]

  val ocb = new Ocb(
    scheduler = context.system.scheduler, 
    maxFailures = 5, 
    callTimeout = 2, 
    resetTimeout = 1)

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
    case msg: TaxRate =>
      db match {
        case None => 
          throw new NullPointerException(ERR_MSG_DB) 
        case Some(db) => 
          ocb.guard(db.run(insertTx(msg))) pipeTo sender()
      }
    case _ =>
  }

  def insertTx(tx: TaxRate): DBIO[Int] =
    sqlu"""insert into taxrate (tx_id, tx_name, tx_rate)
      values (${tx.tx_id}, ${tx.tx_name}, ${tx.tx_rate})"""

  val ERR_MSG_DB = "Connection pool is uninitialized" 
}

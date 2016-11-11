package rcb.tpcdi 

import akka.actor.{ActorLogging, Props}
import akka.pattern.ask
import akka.stream.actor.{ActorSubscriber, WatermarkRequestStrategy}
import akka.stream.actor.ActorSubscriberMessage._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps


class Ingest extends ActorSubscriber with ActorLogging {
  import context.dispatcher

  val requestStrategy = WatermarkRequestStrategy(50)

  implicit val timeout = Timeout(2 seconds)

  val daActor = context.actorOf(Props[DataAccess]) 

  def receive = {
    case OnNext(tr: TaxRate) =>
      ask(daActor, tr) onFailure {
        case _ => log.error("Persistence failed")
      }
    case OnError(err: Exception) => 
      log.error(err, "Receieved exception in stream")
      context.stop(self)
    case OnComplete => 
      log.info("Stream completed!")
      context.stop(self)
    case _ =>
  }
}

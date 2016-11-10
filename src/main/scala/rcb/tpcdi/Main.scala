package rcb.tpcdi

import akka.actor.{ActorSystem, Props}
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import com.typesafe.config.ConfigFactory


object Main extends App {

  val system = ActorSystem("tpcdi", ConfigFactory.load())

  Etl.startEtl(system)
}

object Etl {

  def startEtl(system: ActorSystem) {
    system.log.info("Starting Publisher")
    val publisherActor = system.actorOf(Props[Producer])
    val publisher = ActorPublisher[TaxRate](publisherActor)

    system.log.info("Starting Subscriber")
    val subscriberActor = system.actorOf(Props[Ingest])
    val subscriber = ActorSubscriber[TaxRate](subscriberActor)

    system.log.info("Subscribing to Publisher")
    publisher.subscribe(subscriber)
  }

}

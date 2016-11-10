package com.pcb.db

import akka.actor.Scheduler
import akka.pattern.CircuitBreaker
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class Ocb (
  scheduler: Scheduler, 
  var maxFailures: Int, 
  callTimeout: Int, 
  var resetTimeout: Int)(implicit executor: ExecutionContext) {

  private var cb = newCircuitBreaker

  private def newCircuitBreaker: CircuitBreaker = {
     new CircuitBreaker(
      scheduler,
      maxFailures = maxFailures,
      callTimeout = callTimeout.seconds,
      resetTimeout = resetTimeout.minute)
  }

  def reconfigure(mf: Int, rt: Int): Boolean = {
    var reconfigured = false
    maxFailures = mf
    resetTimeout = rt

    if(cb.isClosed) {
      cb = newCircuitBreaker
      reconfigured = true
    }
   
    reconfigured 
  }  

  def guard[T](body: => Future[T]): Future[T] = cb.withCircuitBreaker(body)
}

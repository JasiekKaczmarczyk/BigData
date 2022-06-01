package Transformations
import CaseClass.{Actors, Names}

object Transformations {
  def heightLessThan(names: Names, height: Integer): Boolean ={
    if(names.height == null) return false
    return names.height <  height
  }

  def specificJob(actors: Actors, job: String): Boolean={
    if(job == null) return false
    return actors.job == job
  }
}



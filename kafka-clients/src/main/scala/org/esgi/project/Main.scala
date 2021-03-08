package org.esgi.project


import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.esgi.project.kafka.MessageProcessing

object Main extends PlayJsonSupport {
  def main(args: Array[String]): Unit = {
    MessageProcessing.run()
  }
}

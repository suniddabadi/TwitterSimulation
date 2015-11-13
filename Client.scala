import akka.actor._
import akka.actor.Actor
import akka.actor.Props
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import scala.collection.mutable.ArrayBuffer
import akka.routing.RoundRobinRouter
import java.net.InetAddress
import scala.util.control.Breaks
import akka.actor.Props
import akka.actor.Actor

import java.security.MessageDigest
import scala.util.control.Breaks
import scala.util.Random
import java.security.MessageDigest

import akka.actor.Scheduler
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledFuture
import akka.actor._
import scala.math._



import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import java.util.concurrent
import java.lang.Runnable
import java.util.concurrent.TimeUnit

import akka.actor.Scheduler
import java.util._

sealed trait TweetMessage
case object Tweet extends TweetMessage
case object RTweet extends TweetMessage
case class Tweet(tweet: String) extends TweetMessage
case class TweetFromUser(tweet:String,userID:String) extends TweetMessage
case class RTweetFromUser(userID:String) extends TweetMessage 
case class GetUserInfo(userID:String) extends TweetMessage
case class UserDetails(info:UserInfo) extends TweetMessage

object Client {

  def main(args: Array[String]) {
    val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
      """akka{
		  		actor{
		  			provider = "akka.remote.RemoteActorRefProvider"
		  		}
		  		remote{
                   enabled-transports = ["akka.remote.netty.tcp"]
		  			netty.tcp{
						hostname = "127.0.0.1"
						port = 1234
					}
				}     
    	}""")

    implicit val system = ActorSystem("Twitter", ConfigFactory.load(config))
    
    val noofclients: Int = args(0).toInt
    val noofUsers: Int = args(1).toInt
    val ip: String = args(2).toString

    val workerRouter = system.actorOf(
      Props(new ConnectServer(ip)).withRouter(RoundRobinRouter(4)), name = "workerRouter")

    

    val noOnthisMachine = noofUsers / noofclients;

    for (i <- 0 to noOnthisMachine / 20) {
      system.actorOf(Props(new UserNode(workerRouter, noofUsers, 100, 1000)), name = i.toString)

    }
    var start = 100
    var count = 0;
    for (i <- 1 + noOnthisMachine / 20 to 3 * noOnthisMachine / 10) {
      system.actorOf(Props(new UserNode(workerRouter, noofUsers, start, 1000)), name = i.toString)
      count = count + 1;
      if (count == noOnthisMachine / 32) {
        count = 0;
        start = start + 1000
      }

    }

    start = 100
    count = 0;
    for (i <- 1 + 3*noOnthisMachine / 10 to noOnthisMachine) {
      system.actorOf(Props(new UserNode(workerRouter, noofUsers, start, 1000)), name = i.toString)
      count = count + 1;
      if (count == noOnthisMachine / 100) {
        count = 0;
        start = start + 1000
      }

    }
  }
}

class ConnectServer(ip: String) extends Actor {

  val connectionPool: Array[ActorRef]=new Array[ActorRef](4)
  for (i <- 1 to 4) {
    connectionPool(i - 1) = context.actorFor("akka.tcp://Twitter@" + ip + ":3035/user/ProxyActor" + i)
  }
  def receive = {
    case Tweet(tweet) => {
        
        var rand= Random.nextInt(4)
        connectionPool(rand) ! TweetFromUser(tweet, sender.path.name)
       
    }
    case RTweet => {
       
        var rand= Random.nextInt(4)
        connectionPool(rand) ! RTweetFromUser(sender.path.name)
      
    }
    case GetUserInfo(userID) => {
      var rand= Random.nextInt(4)
      connectionPool(rand) ! GetUserInfo(sender.path.name)
      
      }

          
    }

}
class UserInfo {

  var followers: HashSet[Int] = new HashSet[Int]
  var following: HashSet[Int] = new HashSet[Int]
  var activity: ArrayList[Long] = new ArrayList[Long]
  var mentions: ArrayList[Long] = new ArrayList[Long]
  var homepage: ArrayList[Long] = new ArrayList[Long]

  def getHomPage(): ArrayList[Long] = {
    return homepage
  }
  def getMentions(): ArrayList[Long] = {
    return mentions
  }
  def getActivity(): ArrayList[Long] = {
    return activity 
  }
  def getFollowers(): HashSet[Int] = {
    return followers
  }
  def getFollowing(): HashSet[Int] = {
    return following
  }
 
}

class UserNode(proxy: ActorRef, noofUsers: Int, startTime: Int, IntervalDuration: Int) extends Actor {
  var text: String = "Depp is regarded as one of the world's biggest movie stars.[2][3] He has gained worldwide critical acclaim for his portrayals of such people as Ed Wood in Ed Wood, Joseph D. Pistone in Donnie Brasco, Hunter S. Thompson in Fear and Loathing in Las Vegas, George Jung in Blow, Jack Sparrow in Pirates of the Caribbean, J.M. Barrie in Finding Neverland, and the Depression Era outlaw John Dillinger in Michael Mann's Public Enemies. Films featuring Depp have grossed over $3.1 billion at the United States box office and over $7.6 billion worldwide.[4] His most commercially successful films are the Pirates of the Caribbean films, which have grossed $3 billion; Alice in Wonderland which grossed $1 billion; Charlie and the Chocolate Factory which grossed $474 million; and The Tourist which grossed $278 million worldwide"
  import context._
  private var scheduler: Cancellable = _
  override def preStart(): Unit = {
    import scala.concurrent.duration._

    scheduler = context.system.scheduler.schedule(Duration.create(startTime, TimeUnit.MILLISECONDS),
      Duration.create(IntervalDuration, TimeUnit.MILLISECONDS), self, Tweet)

  }

  override def postStop(): Unit = {
    scheduler.cancel()
  }

  def receive = {
    case Tweet => {
      var start: Int = 0
      var end: Int = 0
      var i: Int = Random.nextInt(4)
      
      var tweet: String = ""
      (i) match {
        
        case 0 => {
          println("case 0") 
          start = 1+util.Random.nextInt(text.length() - 141)
          end = start + util.Random.nextInt(140)
          tweet = text.substring(start, end);
        }
        //when users put @ in the beginning
        case 1 => {
          println ("case 1 ")
          var rand: Int = 0
          do {
            rand = util.Random.nextInt(noofUsers)
          } while (rand != self.path.name.toInt)
          start = 1+util.Random.nextInt(text.length() - 141)
          end = start + util.Random.nextInt(140 - rand.toString.length() - 3)
          tweet = "@" + rand.toString +" " + text.substring(start, end);
        }
        //when users put @ in the end
        case 2 => {
          println ("case 2 ")
          var rand: Int = 0
          do {
            rand = util.Random.nextInt(noofUsers)
          } while (rand != self.path.name.toInt)
          start = 1+util.Random.nextInt(text.length() - 141)
          end = start + util.Random.nextInt(140 - rand.toString.length() - 3)
           tweet = text.substring(start, end) + "@" + rand.toString + " " ;
          //rintln(tweet)
        }
        case 3 => { 
          proxy ! RTweet
        }
        case 4 =>  { 
          println("Get user info")
          proxy ! GetUserInfo(self.path.name)
        }
      }
    
      proxy ! Tweet(tweet)
     
    }
  }

}
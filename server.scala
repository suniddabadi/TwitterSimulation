import com.typesafe.config.ConfigFactory
import akka.actor._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.routing.RoundRobinRouter
import java.util.concurrent.TimeUnit

import java.net.InetAddress
import java.util.ArrayList
import java.util.HashMap
import java.util.HashSet
import scala.collection.JavaConversions._
import scala.util.Random


sealed trait TweetMessage

case class SendTID(senderID: String, tid: Long, tweetString: String) extends TweetMessage
case class SendFollowersToUpdate(senderID: String, tid: Long, parResult: ParseReturn, set: HashSet[Int]) extends TweetMessage
case class UpdateMentionsNGetFollowers(senderID: String, tid: Long, set: HashSet[Int]) extends TweetMessage
case class SendFollowing(set: HashSet[Int]) extends TweetMessage
case class HomePage(senderID: String, list: ArrayList[Long]) extends TweetMessage
case class UpdateUserHome(tid: Long) extends TweetMessage
case class UpdateUserMentions(tID: Long) extends TweetMessage
case class SendInterSectionSet(senderID: String, tid: Long, intersectionSet: HashSet[Int]) extends TweetMessage
case class UpdateUserHomeNActivity(tid: Long) extends TweetMessage
case class UpdateMasterMap(tid: Long, tweet: String) extends TweetMessage
case class UpdateUserInfo(senderID: String, tid: Long, parResult: ParseReturn) extends TweetMessage
case class UpdateUserInfoRT(senderID: String) extends TweetMessage
case class UpdateFollowing(noofFollowing: Int, totalUsers: Int) extends TweetMessage
case class UpdateFollowers(noofFollowers: Int, totalUsers: Int) extends TweetMessage
case class GetUserInfo(userID: String) extends TweetMessage
case class UserDetails(info: UserInfo) extends TweetMessage
case object GetUserDetails extends TweetMessage
case class Tweet(tweet: String) extends TweetMessage
case class TweetFromUser(tweet: String, userID: String) extends TweetMessage
case class RTweetFromUser(userID: String) extends TweetMessage
case class GetFollowersToUpdate(senderID: String, tid: Long, parResult: ParseReturn) extends TweetMessage
case object GetFollowing extends TweetMessage
case class GetHomePage(senderID: String) extends TweetMessage
case object GetMentions extends TweetMessage
case class GetNextTID(senderID: String, tweetString: String) extends TweetMessage

class UserInfo extends Actor {

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

  def addToHomePage(tweetID: Long) = {
    homepage.add(tweetID)
  }
  def addToActivity(tweetID: Long) = {
    activity.add(tweetID)
  }
  def addToMentions(tweetID: Long) = {
    mentions.add(tweetID)
  }

  def receive = {
    case UpdateUserHome(tid) => {
      addToHomePage(tid)
    }
    case UpdateUserMentions(tID) => {
      addToMentions(tID)
    }
    case UpdateMentionsNGetFollowers(senderID, tid, set) => {
      addToMentions(tid)
      var interSection: HashSet[Int] = new HashSet[Int]
      for (i: Int <- set.toIterator) {
        var temp = i
        if (this.followers.contains(temp)) {
          interSection.add(temp.toInt)
        }
      }
      sender ! SendInterSectionSet(senderID, tid, interSection)
    }
    case UpdateUserHomeNActivity(tID) => {
      addToHomePage(tID)
      addToActivity(tID)
    }
    case UpdateFollowing(noofFollowing: Int, totalUsers: Int) => {
      for (i <- 1 to noofFollowing) {
        following.add(Random.nextInt(totalUsers))
      }
    }
    case UpdateFollowers(noofFollowers: Int, totalUsers: Int) => {
      for (i <- 1 to noofFollowers) {
        followers.add(Random.nextInt(totalUsers))
      }
    }
    case GetUserDetails => {
      println("Print userInfo")
      println("HomePage:")
      for (i: Long <- this.getHomPage()) {
        print(i + " ")
      }
      println("\nActivtiy:")
      for (i: Long <- this.getActivity()) {
        print(i + " ")
      }
      println("\nMentions:")
      for (i: Long <- this.getMentions()) {
        print(i + " ")
      }
    }
    case GetFollowersToUpdate(senderID, tid, parResult) => {
      sender ! SendFollowersToUpdate(senderID, tid, parResult, getFollowers())
    }
    case GetFollowing => {
      sender ! SendFollowing(getFollowing())
    }
    case GetHomePage(senderID) => {
      sender ! HomePage(senderID, getHomPage())
    }
    case GetMentions => {
      sender ! getMentions()
    }
  }
}

object server {

  def main(args: Array[String]): Unit = {

    val hostname = InetAddress.getLocalHost.getHostName
    val config = ConfigFactory.parseString(
      """ 
     akka{ 
    		actor{ 
    			provider = "akka.remote.RemoteActorRefProvider" 
    		} 
    		remote{ 
                enabled-transports = ["akka.remote.netty.tcp"] 
            netty.tcp{ 
    			hostname = "192.168.0.30" 
    			port = 3035 
    		} 
      }      
    }""")

    val noofUsers = Integer.parseInt(args(0))

    val system = ActorSystem("Twitter", ConfigFactory.load(config))

    val master = system.actorOf(Props[Master], name = "master")

    val workerRouter = system.actorOf(
      Props[WorkerActor].withRouter(RoundRobinRouter(16)), name = "workerRouter")

    //Creating the userInfo actors to update the list
    // 10% of users will have  <10 followers && <10 following
    for (i <- 0 to (noofUsers / 10)) {
      var temp = system.actorOf(Props[UserInfo], name = "" + i)
      val following = 1 + Random.nextInt(10)
      val followers = 1 + Random.nextInt(10)
      temp ! UpdateFollowers(followers, noofUsers)
      temp ! UpdateFollowing(following, noofUsers)
    }
    // 10% of users will have  <100 followers && <100 following
    for (i <- (noofUsers / 10) + 1 to 2 * (noofUsers / 10)) {
      var temp = system.actorOf(Props[UserInfo], name = "" + i)
      val following = 95 + Random.nextInt(10)
      val followers = 95 + Random.nextInt(10)
      temp ! UpdateFollowers(followers, noofUsers)
      temp ! UpdateFollowing(following, noofUsers)
    }
    // 50% of users will have  <1000 followers && <1000 following
    for (i <- 2 * (noofUsers / 10) + 1 to 7 * (noofUsers / 10)) {
      var temp = system.actorOf(Props[UserInfo], name = "" + i)
      val following = 950 + Random.nextInt(100)
      val followers = 950 + Random.nextInt(100)
      temp ! UpdateFollowers(followers, noofUsers)
      temp ! UpdateFollowing(following, noofUsers)
    }
    // 20% of users will have  <2500 followers && <2500 following
    for (i <- 7 * (noofUsers / 10) + 1 to 9 * (noofUsers / 10)) {
      var temp = system.actorOf(Props[UserInfo], name = "" + i)
      val following = 2450 + Random.nextInt(100)
      val followers = 2450 + Random.nextInt(100)
      temp ! UpdateFollowers(followers, noofUsers)
      temp ! UpdateFollowing(following, noofUsers)
    }
    // 10% of users will have  <5000 followers && <5000 following
    for (i <- 9 * (noofUsers / 10) + 1 to noofUsers) {
      var temp = system.actorOf(Props[UserInfo], name = "" + i)
      val following = 4950 + Random.nextInt(100)
      val followers = 4950 + Random.nextInt(100)
      temp ! UpdateFollowers(followers, noofUsers)
      temp ! UpdateFollowing(following, noofUsers)
    }

    //Create 4 Proxy Actors as an interface to clients.
    for (i <- 1 until 5) {
      var temp = system.actorOf(Props(new ProxyActor(workerRouter)), name = "ProxyActor" + i)
      println(temp.path.name)
    }

  }

}

class Master extends Actor {

  import context._
  private var scheduler: Cancellable = _
  override def preStart(): Unit = {
    import scala.concurrent.duration._

    scheduler = context.system.scheduler.schedule(Duration.create(100, TimeUnit.MILLISECONDS),
      Duration.create(20000, TimeUnit.MILLISECONDS), self, "PrintCount")

  }

  override def postStop(): Unit = {
    scheduler.cancel()
  }

  var TIDMap: HashMap[Long, String] = new HashMap[Long, String]
  var tid: Long = 0;
  var count: Long = 0;
  def receive = {
    case UpdateMasterMap(tid: Long, tweet: String) => {
      updateTIDMap(tid, tweet)
    }
    case GetNextTID(senderID, tweetString) => {
      
      var id = getNextTID()
      updateTIDMap(id, tweetString)
      sender ! SendTID(senderID, id, tweetString: String)
    }

    case "PrintCount" => {
      println(" Tweet Count after " + count + " secs : " + this.tid)
      count = count + 20
    }
  }

  def getNextTID(): Long = {
    tid = tid + 1;
    return tid;
  }

  def updateTIDMap(key: Long, value: String): Unit = {
    TIDMap.put(key, value)
  }

  def getTweet(TID: Int): String = {
    return TIDMap.get(TID)
  }

}

class ProxyActor(workerRouter: ActorRef) extends Actor {

  def receive = {
    case TweetFromUser(tweet, userID) => {
     
      workerRouter ! TweetFromUser(tweet, userID)
    }
    case RTweetFromUser(userID) => {
      workerRouter ! RTweetFromUser(userID)
    }
    case GetUserInfo(userID) => {
      var sentUser = context.system.actorFor("akka://Twitter/user/" + userID)
      
      sentUser ! GetUserDetails
    }
    case UserDetails(info: UserInfo) => {
    }
  }
}

class ParseReturn(par: Int, rep: Int) {
  var x: Int = par
  var y: Int = rep
  def getX(): Int = { return x }
  def getY(): Int = { return y }
}

class WorkerActor extends Actor {
  val worker = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(16)), name = "worker")

  def receive = {
    case TweetFromUser(tweet, userID) => {
      
      val master = context.system.actorFor("akka://Twitter/user/master");
      master ! GetNextTID(userID, tweet)
    }
    case RTweetFromUser(userID) => {
      worker ! UpdateUserInfoRT(userID)
    }
    case SendTID(senderID, tid, tweetString) => {
     
      worker ! UpdateUserInfo(senderID, tid, parseTweet(tweetString))
    }
  }

  def parseTweet(tweet: String): ParseReturn = {
    var response: ParseReturn = null
   
    var ch: Array[Char] = tweet.toCharArray()
    var i: Int = 0;
    if (ch.length != 0) {
      if (ch(0) == '@') {
        i = i + 1;
        while (i < ch.length && ch(i) != ' ') {
          i = i + 1;
        }
        var y: Int = 1;
        try {
          y = Integer.parseInt(tweet.substring(1, i))
        } catch {
          case e: NumberFormatException => {
            return new ParseReturn(0, 0)
          }
        }
        response = new ParseReturn(2, y) // case 2 to update the intersection
      }
      if (response != null) {
        i = 0;
        while (i < ch.length && ch(i) != '@') { i = i + 1 }
        if (i == ch.length) response = new ParseReturn(0, 0) // no mentions.
        else {
          var j = i
          while (j < ch.length && ch(j) != ' ') {
            j = j + 1;
          }
          var y: Int = 0
          try {
            y = Integer.parseInt(tweet.substring(i, j))
          } catch {
            case e: NumberFormatException => {
              return new ParseReturn(0, 0);
            }
          }
          response = new ParseReturn(1, y); // case 1
        }
      }
    } else {
      response = new ParseReturn(-1, -1);
    }
    return response;
  }
}

class Worker extends Actor {
  def receive = {
    case UpdateUserInfo(senderID: String, tid: Long, parResult: ParseReturn) => {
      var sentUser = context.system.actorFor("akka://Twitter/user/" + senderID)
      sentUser ! UpdateUserHomeNActivity(tid)
      var x = parResult.getX()
     
      if (x != -1) { //No mentions && 
        sentUser ! GetFollowersToUpdate(senderID, tid, parResult)
      }

    }
    case UpdateUserInfoRT(senderID) => {
      var sentUser = context.system.actorFor("akka://Twitter/user/" + senderID)
      sentUser ! GetHomePage(senderID)
    }
    case HomePage(senderID, list) => {
      var n = list.size()
      if (n > 0) {
        var rand = Random.nextInt(n)
        val randTID = list(rand)
        sender ! UpdateUserHomeNActivity(randTID)
        val parResult = new ParseReturn(0, 0)
        sender ! GetFollowersToUpdate(senderID, randTID, parResult)
      }
    }
    case SendInterSectionSet(senderID, tid, intersectionSet) => {
      for (i: Int <- intersectionSet.toIterator) {
        var temp = i
        var tempUser = context.system.actorFor("akka://Twitter/user/" + temp)
        tempUser ! UpdateUserHome(tid)
      }
    }
    case SendFollowersToUpdate(senderID: String, tid: Long, parResult: ParseReturn, set: HashSet[Int]) => {
      var x = parResult.getX()
      var y = parResult.getY()

      if (x == 0 || x == 1) {
        for (i: Int <- set.toIterator) {
          var temp = i
          var tempUser = context.system.actorFor("akka://Twitter/user/" + temp)
          tempUser ! UpdateUserHome(tid)
        }
      }
      val user = context.system.actorFor("akka://Twitter/user/" + y)
      if (x == 1) {
       
        user ! UpdateUserMentions(tid)
      }
      if (x == 2) {
        
        user ! UpdateMentionsNGetFollowers(senderID, tid, set)
      }

    }
  }
}
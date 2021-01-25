import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

case class Client(id: String, name: String, inboundFeedUrl: String, jobGroups: List[JobGroup])
case class JobGroup(allOrAny: String, id: String, rules: List[Rule], sponsoredPublishers: List[Publisher])
case class Publisher(id: String, isActive: Boolean, var outboundFileName: List[Job])

case class Rule(allOrAny: String, jobAttribute: String, operator: String, data: String)

case class Job( title: String, company: String, city: String, state: String,
                country: String, description: String, id: Int,
                url: String, date: String, cpcBid: Int, cpaBid : Int)

object MainJobFunction extends App {

  implicit val formats = DefaultFormats
  
  def getEntity[T](path: String)(implicit m: Manifest[T]): T = {
    val entitySrc = scala.io.Source.fromFile(path)
    val entityStr = try entitySrc.mkString finally entitySrc.close()
    JsonMethods.parse(entityStr).extract[T]
  }

  // SCANNED JOBS ARE GETTING STORED HERE IN FORM OF LIST OF JOBS
  val scannedJobs = getEntity[List[Job]]("InboundFeedByClient.json")

  // DEFINING A PUBLISHER HERE
  var empty = List[Job]()
  val PublisherReceivingOutboundFeedWithAny = Publisher("PublisherReceivingOutboundFeed", isActive = true, empty)
  val PublisherReceivingOutBoundFeedWithAll = Publisher("PublisherReceivingOutboundFeedAllCondition", isActive = true, empty)
  // DEFINING A LIST OF RULES WITH ANY OF THE FOLLOWING RULES GETTING PASSED

  val setOfRulesWithAny = List(Rule("any", "state", "equal", "London"),Rule("any", "city", "equal", "London"),
    Rule("any","country", "equal", "United States"), Rule("any", "cpcBid", "less than equal", "33"),
    Rule("any", "cpaBid", "greater than equal", "90"))

  val setOfRulesWithAll = List(Rule("all", "state", "equal", "London"), Rule("all", "city", "not equal", "India"),
    Rule("all", "cpcBid", "less than equal", "47"), Rule("all", "cpaBid", "greater than equal", "90"))

  val jobGroupWithAnyCondition = JobGroup("any", "JobGroupWithAny", setOfRulesWithAny, List(PublisherReceivingOutboundFeedWithAny))
  val jobGroupWithAllCondition = JobGroup("all", "JobGroupWithAll", setOfRulesWithAll, List(PublisherReceivingOutBoundFeedWithAll))

  val groupOfJobsPassingRulesSuccessfully = MainJobsFilter.jobsFilter(jobGroupWithAnyCondition, scannedJobs)
  val finalJobsAfterFilter = MainJobsFilter.jobsFilter(jobGroupWithAllCondition, scannedJobs)

  JsonWriter.writeJson(groupOfJobsPassingRulesSuccessfully, "PublisherReceivingOutboundFeed.json")
  JsonWriter.writeJson(finalJobsAfterFilter, "OutboundFeedWithAllCondition.json")
}
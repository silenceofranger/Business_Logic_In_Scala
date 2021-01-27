import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

import scala.collection.IterableOnce.iterableOnceExtensionMethods
import scala.collection.mutable.ListBuffer

case class Client(id: String, name: String, inboundFeedUrl: String, jobGroups: List[JobGroup])

// ********************************* DEFINED A NEW JOB GROUP TO ATTACH PRIORITY TO IT ************************************
case class jobGroupCaseClassWithPriority(priority: Int, jobGroupWithPriority: List[Rule])

case class priorityAndJobsList(priority: Int, var jobs: List[Job])

case class JobGroup(allOrAny: String, id: String, jobGroupWithPriority: jobGroupCaseClassWithPriority, sponsoredPublishers: List[Publisher])

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

  // ******************** SCANNED JOBS ARE GETTING STORED HERE IN FORM OF LIST OF JOBS *********************
  val scannedJobs = getEntity[List[Job]]("InboundFeedByClient.json")

  // DEFINING A PUBLISHER HERE
  var empty = List[Job]()
  val publisher1 = Publisher("Publisher1", isActive = true, empty)
  val publisher2 = Publisher("Publisher2", isActive = true, empty)
  val publisher3 = Publisher("Publisher3", isActive = true, empty)
  val publisher4 = Publisher("Publisher4", isActive = true, empty)

  // DEFINING A LIST OF RULES WITH ANY OF THE FOLLOWING RULES GETTING PASSED
  // DEFINING A CASE CLASS WHICH HAVE PRIORITY ATTACHED WITH IT

  //  val setOfRulesWithAll = List(Rule("all", "state", "equal", "London"), Rule("all", "city", "not equal", "India"),
  //    Rule("all", "cpcBid", "less than equal", "47"), Rule("all", "cpaBid", "greater than equal", "90"))

  //  val jobGroupWithAnyCondition = JobGroup("any", "JobGroupWithAny", setOfRulesWithAny, List(PublisherReceivingOutboundFeedWithAny))
  //  val jobGroupWithAllCondition = JobGroup("all", "JobGroupWithAll", setOfRulesWithAll, List(PublisherReceivingOutBoundFeedWithAll))

  //  val groupOfJobsPassingRulesSuccessfully = MainJobsFilter.jobsFilter(jobGroupWithAnyCondition, scannedJobs)
  //  val finalJobsAfterFilter = MainJobsFilter.jobsFilter(jobGroupWithAllCondition, scannedJobs)

  val setOfRulesWithAny = List(Rule("any", "state", "equal", "London"),/*Rule("any", "city", "equal", "London"),
    Rule("any","country", "equal", "United States"), Rule("any", "cpcBid", "less than equal", "33"),
    Rule("any", "cpaBid", "greater than equal", "90")*/)


  val jobGroupOneWithPriorityAndRules = jobGroupCaseClassWithPriority(3, setOfRulesWithAny)
  val jobGroupTwoWithPriorityAndRules = jobGroupCaseClassWithPriority(4, setOfRulesWithAny)


  val jobGroupOne = JobGroup("any", "jobGroupOne", jobGroupOneWithPriorityAndRules, List(publisher1, publisher2, publisher3))
  val jobGroupTwo = JobGroup("any", "jobGroupTwo", jobGroupTwoWithPriorityAndRules, List(publisher1, publisher4))


  val jobsInOne = MainJobsFilter.jobsFilter(jobGroupOne, scannedJobs)
  val jobsInTwo = MainJobsFilter.jobsFilter(jobGroupTwo, scannedJobs)


  val priorityAndListOfJobsMap = scala.collection.mutable.Map(jobGroupOne -> jobsInOne )
  priorityAndListOfJobsMap += (jobGroupTwo -> jobsInTwo)


  // ******************************************* PRIORITY IMPLEMENTATION ***********************************************

  for((jobGroupName,priorityWithJobs) <- priorityAndListOfJobsMap) {
    val currentPriority = priorityWithJobs.priority
    val currentJobs = priorityWithJobs.jobs
    val currentJobGroup = jobGroupName
    for(job <- currentJobs) {

      for((jobGroupName,priorityWithJobs) <- priorityAndListOfJobsMap) {
        val priority = priorityWithJobs.priority
        val jobGroup = jobGroupName

        for(tempJob <- priorityWithJobs.jobs) {
          if(tempJob == job && currentJobGroup != jobGroup && priority <= currentPriority)
            {
              // ELIMINATING DUPLICATE JOBS HERE
              val answerList = priorityWithJobs.jobs.filter( _ != tempJob )
              priorityWithJobs.jobs = answerList
            }
        }
      }
    }
  }

  for((jobGroupName, priorityWithJobs) <- priorityAndListOfJobsMap) {
    for(job <- priorityWithJobs.jobs)
      {
        println(s"$jobGroupName   ======>     $job")
      }
  }

  for((jobGroupName, priorityWithJobs) <- priorityAndListOfJobsMap) {
    val publisherList = jobGroupName.sponsoredPublishers
    for(publisher <- publisherList) {
      for (job <- priorityWithJobs.jobs) {
        val x = priorityWithJobs.jobs.filter( _ == job)
        publisher.outboundFileName = publisher.outboundFileName ++ x
      }
    }
  }

  // PASSING JOBS TO PUBLISHER AFTER CREATION OF JOB GROUPS AND ONLY JOBS LEFT IN JOB GROUPS ARE GETTING ASSIGNED TO RESPECTIVE PUBLISHERS

    JsonWriter.writeJson(publisher1.outboundFileName, "Publisher1.json")
    JsonWriter.writeJson(publisher2.outboundFileName, "Publisher2.json")
    JsonWriter.writeJson(publisher3.outboundFileName, "Publisher3.json")
    JsonWriter.writeJson(publisher4.outboundFileName, "Publisher4.json")

}
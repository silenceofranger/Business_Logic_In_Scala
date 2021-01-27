object MainJobsFilter {

  def jobsFilter(setOfRules: JobGroup, jobsList: List[Job]): priorityAndJobsList = {

    val allOrAny = setOfRules.allOrAny
    var appendingFinalJobs = List[Job]()
    val rules = setOfRules.jobGroupWithPriority.jobGroupWithPriority
    val priority = setOfRules.jobGroupWithPriority.priority
    var scannedInboundFeed = jobsList
    var storingFinalResult = jobsList

    // INTERSECTION OF JOB GROUP CONDITIONS IN THIS SECTION
    if(allOrAny == "all")
      {
        var jobsOfFirstCondition = List[Job]()
        var jobsOfSecondCondition = List[Job]()
        var jobsOfThirdCondition = List[Job]()
        var jobsOfFourthCondition = List[Job]()

//        println("COMING HERE")
        for(conditionsOfRules <- rules) {

          if (conditionsOfRules.operator == "less than equal") {
            if (conditionsOfRules.jobAttribute == "cpcBid") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.cpcBid <= conditionsOfRules.data.toInt)
              jobsOfFirstCondition = jobsOfFirstCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "cpaBid") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.cpaBid <= conditionsOfRules.data.toInt)
              jobsOfFirstCondition = jobsOfFirstCondition ++ storingTemporaryJobs
            }

          }
          else if (conditionsOfRules.operator == "greater than equal") {
            if (conditionsOfRules.jobAttribute == "cpcBid") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.cpcBid >= conditionsOfRules.data.toInt)
              jobsOfSecondCondition = jobsOfSecondCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "cpaBid") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.cpaBid >= conditionsOfRules.data.toInt)
              jobsOfSecondCondition = jobsOfSecondCondition ++ storingTemporaryJobs
            }
          }
//                    println(jobsOfFirstCondition)
//                    println(jobsOfSecondCondition)

          // ************************************** EQUAL OPERATOR ************************************************
          else if (conditionsOfRules.operator == "equal") {

            if (conditionsOfRules.jobAttribute == "title") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.title == conditionsOfRules.data)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "company") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.company == conditionsOfRules.data)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "city") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.city == conditionsOfRules.data)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "state") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.state == conditionsOfRules.data)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "country") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.country == conditionsOfRules.data)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "description") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.description == conditionsOfRules.data)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "id") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.id == conditionsOfRules.data.toInt)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "url") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.url == conditionsOfRules.data)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "date") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.date == conditionsOfRules.data)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "cpcBid") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.cpcBid == conditionsOfRules.data.toInt)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "cpaBid") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.cpaBid == conditionsOfRules.data.toInt)
              jobsOfThirdCondition = jobsOfThirdCondition ++ storingTemporaryJobs
            }
            else {
              println("CHECK FOR THE ATTRIBUTE IN INBOUND FEED OF JOBS")
            }
          }
            // ************************************** NOT EQUAL OPERATOR ***********************************************
          else if (conditionsOfRules.operator == "not equal") {
            if (conditionsOfRules.jobAttribute == "title") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.title != conditionsOfRules.data)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "company") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.company != conditionsOfRules.data)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "city") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.city != conditionsOfRules.data)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "state") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.state != conditionsOfRules.data)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "country") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.country != conditionsOfRules.data)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "description") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.description != conditionsOfRules.data)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "id") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.id != conditionsOfRules.data.toInt)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "url") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.url != conditionsOfRules.data)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "date") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.date != conditionsOfRules.data)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "cpcBid") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.cpcBid != conditionsOfRules.data.toInt)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else if (conditionsOfRules.jobAttribute == "cpaBid") {
              val storingTemporaryJobs = scannedInboundFeed.filter(_.cpaBid != conditionsOfRules.data.toInt)
              jobsOfFourthCondition = jobsOfFourthCondition ++ storingTemporaryJobs
            }
            else {
              println("CHECK FOR THE ATTRIBUTE IN INBOUND FEED OF JOBS")
            }
          }
          else {
            println("CHECK FOR THE OPERATOR USED IN JOB GROUP")
          }

          storingFinalResult = jobsOfFirstCondition.intersect(jobsOfSecondCondition).intersect(jobsOfThirdCondition)
            .intersect(jobsOfFourthCondition)

        }
        val answer = priorityAndJobsList(priority, storingFinalResult)
        return answer
      }




    // UNION OF JOB GROUP CONDITIONS IN THIS SECTION
    else if(allOrAny == "any") {
      for (conditionsOfRules <- rules) {
        if (conditionsOfRules.operator == "less than equal") {
          if (conditionsOfRules.jobAttribute == "cpcBid") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.cpcBid <= conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "cpaBid") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.cpaBid <= conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else {
            println("CHECK FOR THE CPC BID AND CPA BID ATTRIBUTE IN INBOUND FEED OF JOBS")
          }
        }
        else if (conditionsOfRules.operator == "greater than equal") {
          if (conditionsOfRules.jobAttribute == "cpcBid") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.cpcBid >= conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "cpaBid") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.cpaBid >= conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else {
            println("CHECK FOR THE CPC BID AND CPA BID ATTRIBUTE IN INBOUND FEED OF JOBS")
          }
        }


        else if (conditionsOfRules.operator == "equal") {
          if (conditionsOfRules.jobAttribute == "title") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.title == conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "company") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.company == conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "city") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.city == conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "state") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.state == conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "country") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.country == conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "description") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.description == conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "id") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.id == conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "url") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.url == conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "date") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.date == conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "cpcBid") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.cpcBid == conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "cpaBid") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.cpaBid == conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else {
            println("CHECK FOR THE ATTRIBUTE IN INBOUND FEED OF JOBS")
          }
        }

        else if (conditionsOfRules.operator == "not equal") {
          if (conditionsOfRules.jobAttribute == "title") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.title != conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "company") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.company != conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "city") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.city != conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "state") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.state != conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "country") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.country != conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "description") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.description != conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "id") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.id != conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "url") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.url != conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "date") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.date != conditionsOfRules.data)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "cpcBid") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.cpcBid != conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else if (conditionsOfRules.jobAttribute == "cpaBid") {
            val storingTemporaryJobs = scannedInboundFeed.filter(_.cpaBid != conditionsOfRules.data.toInt)
            appendingFinalJobs = appendingFinalJobs ++ storingTemporaryJobs
          }
          else {
            println("CHECK FOR THE ATTRIBUTE IN INBOUND FEED OF JOBS")
          }
        }
        else {
          println("CHECK FOR THE OPERATOR USED IN JOB GROUP")
        }

      }
    }
    storingFinalResult = appendingFinalJobs

    val answer = priorityAndJobsList(priority, storingFinalResult)
    answer
  }

}


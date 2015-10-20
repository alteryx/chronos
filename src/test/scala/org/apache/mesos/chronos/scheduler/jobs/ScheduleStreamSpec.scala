package org.apache.mesos.chronos.scheduler.jobs

import org.joda.time._
import org.specs2.mutable._

class ScheduleStreamSpec extends SpecificationWithJUnit {

  val fakeCurrentTime = DateTime.parse("2012-01-01T00:00:00Z")

  "ScheduleStream" should {
    "return a properly clipped schedule" in {
      val orgSchedule = Schedules.parseIso8601Schedule("R3/2012-01-01T00:00:00.000Z/P1D").get
      val stream = new ScheduleStream(null, orgSchedule)
      stream.head must_== (null, orgSchedule)

      val nextSchedule = stream.tail.get.head._2
      nextSchedule.invocationTime must_== Schedules.parseIso8601Schedule("R2/2012-01-02T00:00:00.000Z/P1D").get.invocationTime
      nextSchedule.asInstanceOf[Iso8601Schedule].offset must_== 1
      nextSchedule.recurrences must_== Some(2)

      val nextSchedule2 = stream.tail.get.tail.get.head._2
      nextSchedule2.invocationTime must_== Schedules.parseIso8601Schedule("R1/2012-01-03T00:00:00.000Z/P1D").get.invocationTime
      nextSchedule2.asInstanceOf[Iso8601Schedule].offset must_== 2
      nextSchedule2.recurrences must_== Some(1)

      val nextSchedule3 = stream.tail.get.tail.get.tail.get.head._2
      nextSchedule3.invocationTime must_== Schedules.parseIso8601Schedule("R0/2012-01-04T00:00:00.000Z/P1D").get.invocationTime
      nextSchedule3.asInstanceOf[Iso8601Schedule].offset must_== 3
      nextSchedule3.recurrences must_== Some(0)

      nextSchedule3.next must_== None
    }

    "return a infinite schedule when no repetition is specified" in {
      val orgSchedule = Schedules.parseIso8601Schedule("R/2012-01-01T00:00:00.000Z/P1D").get
      val stream = new ScheduleStream(null, orgSchedule)
      val infiniteSchedule = stream.head._2
      infiniteSchedule.invocationTime must_== orgSchedule.invocationTime
      infiniteSchedule.recurrences must_== None

      stream.tail.get.head._2.invocationTime must_== Schedules.parseIso8601Schedule("R/2012-01-02T00:00:00.000Z/P1D").get.invocationTime
    }

    "be convertible back to a string for simple cases" in {
      val orgSchedule = new ScheduleStream(null, Schedules.parseIso8601Schedule("R/2012-01-01T00:00:00.000Z/P1D").get)
      val newSchedule = orgSchedule.tail.get.tail.get.head._2

      newSchedule.toStringRepresentation must_== "R/2012-01-03T00:00:00.000Z/P1D"
    }

    "handle variable length months repetition correctly" in {
      val orgSchedule = Schedules.parseIso8601Schedule("R/2012-01-31T00:00:00.000Z/P1M").get
      val stream = new ScheduleStream(null, orgSchedule)

      val nextSchedule = stream.tail.get.head._2
      nextSchedule.invocationTime must_== Schedules.parseIso8601Schedule("R/2012-02-29T00:00:00.000Z/P1M").get.invocationTime
      nextSchedule.asInstanceOf[Iso8601Schedule].offset must_== 1
      nextSchedule.recurrences must_== None

      val nextSchedule2 = stream.tail.get.tail.get.head._2
      nextSchedule2.invocationTime must_== Schedules.parseIso8601Schedule("R/2012-03-31T00:00:00.000Z/P1M").get.invocationTime
      nextSchedule2.asInstanceOf[Iso8601Schedule].offset must_== 2

      val nextSchedule3 = stream.tail.get.tail.get.tail.get.head._2
      nextSchedule3.invocationTime must_== Schedules.parseIso8601Schedule("R/2012-04-30T00:00:00.000Z/P1M").get.invocationTime
      nextSchedule3.asInstanceOf[Iso8601Schedule].offset must_== 3
    }

    "Generate cron schedules the same way" in {

      def utcTime(s: String) = DateTime.parse(s).withZoneRetainFields(DateTimeZone.UTC)

      val now = utcTime("2015-10-19T14:00:00.000Z")

      val oneAM = Schedules.parseCronSchedule("0 1 * * *", currentTime = now).get
      val stream = new ScheduleStream("name", oneAM)

      stream.head._2.invocationTime must_== utcTime("2015-10-20T01:00:00.000Z")
      stream.tail.get.head._2.invocationTime must_== utcTime("2015-10-21T01:00:00.000Z")
      stream.tail.get.tail.get.head._2.invocationTime must_== utcTime("2015-10-22T01:00:00.000Z")
    }

    "Generate cron schedules for other time zones" in {
      def chicagoTime(s: String) = DateTime.parse(s).withZoneRetainFields(DateTimeZone.forID("US/Central"))

      val now = chicagoTime("2015-10-19T14:00:00.000Z")

      val oneAM = Schedules.parseCronSchedule("0 1 * * *", timeZoneStr = "US/Central", currentTime = now).get
      val stream = new ScheduleStream("name", oneAM)

      stream.head._2.invocationTime must_== chicagoTime("2015-10-20T01:00:00.000Z").withZone(DateTimeZone.UTC)
      stream.tail.get.head._2.invocationTime must_== chicagoTime("2015-10-21T01:00:00.000Z").withZone(DateTimeZone.UTC)
      stream.tail.get.tail.get.head._2.invocationTime must_== chicagoTime("2015-10-22T01:00:00.000Z").withZone(DateTimeZone.UTC)
    }
  }
}

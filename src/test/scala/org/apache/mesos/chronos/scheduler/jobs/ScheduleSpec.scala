package org.apache.mesos.chronos.scheduler.jobs

import org.joda.time.{DateTime, DateTimeZone}
import org.specs2.mutable.SpecificationWithJUnit

/**
  */
class ScheduleSpec extends SpecificationWithJUnit {

  val fakeCurrentTime = DateTime.parse("2012-01-01T00:00:00Z")

  "Schedules" should {
    "ISO: Parse correctly" in {
      val maybeSchedule = Schedules.parse("R3/2012-01-01T00:00:00.000Z/P1D", scheduleType = Iso8601Type)

      maybeSchedule.isDefined must_== true
      val asIso = maybeSchedule.get.asInstanceOf[Iso8601Schedule]

      asIso.toStringRepresentation must_== "R3/2012-01-01T00:00:00.000Z/P1D"
    }

    "ISO: Fail to parse" in {
      val failedSchedule = Schedules.parse("lol not a schedule", scheduleType = Iso8601Type)
      failedSchedule.isDefined must_== false
    }

    "ISO: Handle getting the next occurance properly" in {
      val maybeSchedule = Schedules.parse("R3/2012-01-01T00:00:00.000Z/P1D", scheduleType = Iso8601Type)

      maybeSchedule.isDefined must_== true
      val asIso = maybeSchedule.get.asInstanceOf[Iso8601Schedule]

      asIso.invocationTime must_== DateTime.parse("2012-01-01T00:00:00.000Z")
      asIso.next.get.invocationTime must_== DateTime.parse("2012-01-02T00:00:00.000Z")
    }

    "Cron: Parse correctly" in {
      val maybeSchedule = Schedules.parse("* 0 * * *", scheduleType = CronType)

      maybeSchedule.isDefined must_== true
      val asCron = maybeSchedule.get.asInstanceOf[CronSchedule]

      asCron.toStringRepresentation must_== "* 0 * * *"
    }

    "Cron: Fail to parse" in {
      val cron = Schedules.parse("1 1 FOOBAR * *", scheduleType = CronType)

      cron.isDefined must_== false
    }

    "Cron: Handle getting the next occurance properly" in {

      val now = DateTime.parse("2012-02-29T01:00:00.000Z").withZoneRetainFields(DateTimeZone.UTC)
      val maybeSchedule = Schedules.parse("0 0 * * *", currentTime = now, scheduleType = CronType)

      maybeSchedule.isDefined must_== true
      val asCron = maybeSchedule.get.asInstanceOf[CronSchedule]

      val expectedNext = DateTime.parse("2012-03-01T00:00:00.000Z").withZoneRetainFields(DateTimeZone.UTC)
      asCron.next.get.invocationTime must_== expectedNext
    }

    "Cron: handle time zones" in {
      val now = DateTime.parse("2012-02-29T01:00:00.000Z").withZoneRetainFields(DateTimeZone.forID("US/Chicago"))
      val maybeSchedule = Schedules.parse("0 0 * * *", currentTime = now, scheduleType = CronType)

      maybeSchedule.isDefined must_== true
      val asCron = maybeSchedule.get.asInstanceOf[CronSchedule]

      val expectedNext = DateTime.parse("2012-03-01T00:00:00.000Z").withZoneRetainFields(DateTimeZone.forID("US/Chicago"))
      asCron.next.get.invocationTime must_== expectedNext
    }
  }
}
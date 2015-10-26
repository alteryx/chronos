package org.apache.mesos.chronos.scheduler.jobs

import java.util.logging.Logger

import com.cronutils.model.{Cron, CronType => CronTypeEnum}
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import com.fasterxml.jackson.annotation.JsonProperty
import org.joda.time.{DateTimeZone, DateTime, Period}

import scala.util.{Failure, Success, Try}

/**
 * Base trait for all schedule types
 */
sealed trait Schedule extends Product { self =>

  type SType <: Schedule { type SType = self.SType }

  def schedule: String
  def scheduleTimeZone: String
  def scheduleType: ScheduleType
  def invocationTime: DateTime
  def recurrences: Option[Long]
  def next: Option[SType]
  def toStringRepresentation: String
}

/**
 * A schedule that uses ISO 8601 repeating interval notation
 *
 * @param schedule the text representation of the schedule
 * @param scheduleTimeZone the time zone the schedule should be used in
 * @param invocationTime the time this schedule needs to be invoked (UTC)
 * @param originTime the root time that the ISO 8601 schedule was started (UTC)
 * @param offset the number of executions after the origin that have completed
 * @param recurrences the number of remaining executions, or -1 if this is an infinite schedule
 * @param period the time between executions
 */
case class Iso8601Schedule(@JsonProperty schedule: String,
                           @JsonProperty scheduleTimeZone: String,
                           @JsonProperty invocationTime: DateTime,
                           @JsonProperty originTime: DateTime,
                           @JsonProperty offset: Long = 0,
                           @JsonProperty recurrences: Option[Long] = None,
                           @JsonProperty period: Period) extends Schedule {
  type SType = Iso8601Schedule

  val scheduleType = Iso8601Type

  override def next: Option[Iso8601Schedule] = {
    if (recurrences.exists(_ == 0)) None
    else {
      val nextOffset = offset + 1
      val nextInvocationTime = Schedules.addPeriods(originTime, period, nextOffset.asInstanceOf[Int] /* jodatime doesn't support longs in their plus methods. What to do about overflow? */)

      Some(Iso8601Schedule(schedule, scheduleTimeZone, nextInvocationTime, originTime, nextOffset, recurrences.map(_ - 1), period))
    }
  }

  def toStringRepresentation: String = {
    Iso8601Expressions.create(recurrences.getOrElse(-1), invocationTime, period)
  }
}

/**
 * A Unix Cron schedule
 *
 * @param schedule the text representation of this schedule
 * @param scheduleTimeZone the time zone the schedule should be executed from
 * @param invocationTime the time to execute this schedule (UTC)
 * @param lastExecutionTime the last time this schedule was run (UTC)
 * @param cron the parsed Cron object that is used to perform execution time planning
 */
case class CronSchedule(@JsonProperty schedule: String,
                        @JsonProperty scheduleTimeZone: String,
                        @JsonProperty invocationTime: DateTime,
                        @JsonProperty lastExecutionTime: DateTime,
                         cron: Cron) extends Schedule {
  type SType = CronSchedule
  val recurrences = None
  val scheduleType = CronType

  lazy val next: Option[CronSchedule] = {
    val tz = if(scheduleTimeZone != "") DateTimeZone.forID(scheduleTimeZone) else DateTimeZone.UTC
    val currentTimeUserTZ = invocationTime.withZone(tz)
    val nextExecUserTZ = ExecutionTime.forCron(cron).nextExecution(currentTimeUserTZ)
    val nextExec = nextExecUserTZ.withZone(DateTimeZone.UTC)

    Some(copy(invocationTime = nextExec, lastExecutionTime = invocationTime))
  }

  def toStringRepresentation = cron.asString()
}

object Schedules {
  private val log = Logger.getLogger(getClass.getName)
  private val cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronTypeEnum.UNIX)

  /**
   * Parses a text representation of a schedule into a Schedule object, or None if the parsing fails
   * @param scheduleStr the string representation of the schedule
   * @param timeZoneStr the time zone to execute the schedule in
   * @param scheduleType the type of schedule (Iso8601Type or CronType)
   * @param currentTime the current DateTime in UTC
   * @return
   */
  def parse(scheduleStr: String,
            timeZoneStr: String = "",
            scheduleType: ScheduleType = Iso8601Type,
            currentTime: DateTime = DateTime.now(DateTimeZone.UTC)): Option[Schedule] = {

    scheduleType match {
      case Iso8601Type => parseIso8601Schedule(scheduleStr, timeZoneStr)
      case CronType => parseCronSchedule(scheduleStr, timeZoneStr, currentTime)
    }
  }

  def parseIso8601Schedule(scheduleStr: String, timeZoneStr: String = ""): Option[Iso8601Schedule] = {
    Iso8601Expressions.parse(scheduleStr, timeZoneStr).map {
      case (recurrences, start, period) =>
        Iso8601Schedule(scheduleStr, timeZoneStr, start, start, offset = 0, if (recurrences == -1) None else Some(recurrences), period)
    }
  }

  def parseCronSchedule(scheduleStr: String, timeZoneStr: String = "UTC", currentTime: DateTime = DateTime.now(DateTimeZone.UTC)): Option[CronSchedule] = {
    Try(parseCron(scheduleStr)) match {
      case Failure(e) =>
        log.warning(s"Failed to parse cron schedule: $scheduleStr, error was ${e.toString}")
        None

      case Success(cron) =>
        CronSchedule(scheduleStr, timeZoneStr, currentTime, currentTime, cron).next
    }
  }

  def parseCron(scheduleStr: String): Cron = {
    new CronParser(cronDefinition).parse(scheduleStr)
  }

  // replace with multiplied by?
  def addPeriods(origin: DateTime, period: Period, number: Int): DateTime = {
    origin.plus(period.multipliedBy(number))
  }
}

object ScheduleType {
  def parse(s: String): Option[ScheduleType] = List(Iso8601Type, CronType).find(_.name.toLowerCase == s.toLowerCase)
}

sealed trait ScheduleType {
  def name: String
}

case object Iso8601Type extends ScheduleType {
  val name = "Iso8601"
}

case object CronType extends ScheduleType {
  val name = "Cron"
}
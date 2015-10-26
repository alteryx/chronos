package org.apache.mesos.chronos.utils

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import org.apache.mesos.chronos.scheduler.jobs.constraints.{EqualsConstraint, LikeConstraint}
import org.apache.mesos.chronos.scheduler.jobs._
import org.joda.time.format.{ISOPeriodFormat, ISODateTimeFormat}

/**
 * Custom JSON serializer for jobs.
 * @author Florian Leibert (flo@leibert.de)
 */
class JobSerializer extends JsonSerializer[BaseJob] {

  def serialize(baseJob: BaseJob, json: JsonGenerator, provider: SerializerProvider) {
    json.writeStartObject()
    json.writeFieldName("name")
    json.writeString(baseJob.name)

    json.writeFieldName("command")
    json.writeString(baseJob.command)

    json.writeFieldName("shell")
    json.writeBoolean(baseJob.shell)

    json.writeFieldName("epsilon")
    json.writeString(baseJob.epsilon.toString)

    json.writeFieldName("executor")
    json.writeString(baseJob.executor)

    json.writeFieldName("executorFlags")
    json.writeString(baseJob.executorFlags)

    json.writeFieldName("retries")
    json.writeNumber(baseJob.retries)

    json.writeFieldName("owner")
    json.writeString(baseJob.owner)

    json.writeFieldName("ownerName")
    json.writeString(baseJob.ownerName)

    json.writeFieldName("description")
    json.writeString(baseJob.description)

    json.writeFieldName("async")
    json.writeBoolean(baseJob.async)

    json.writeFieldName("successCount")
    json.writeNumber(baseJob.successCount)

    json.writeFieldName("errorCount")
    json.writeNumber(baseJob.errorCount)

    json.writeFieldName("lastSuccess")
    json.writeString(baseJob.lastSuccess)

    json.writeFieldName("lastError")
    json.writeString(baseJob.lastError)

    json.writeFieldName("cpus")
    json.writeNumber(baseJob.cpus)

    json.writeFieldName("disk")
    json.writeNumber(baseJob.disk)

    json.writeFieldName("mem")
    json.writeNumber(baseJob.mem)

    json.writeFieldName("disabled")
    json.writeBoolean(baseJob.disabled)

    json.writeFieldName("softError")
    json.writeBoolean(baseJob.softError)

    json.writeFieldName("dataProcessingJobType")
    json.writeBoolean(baseJob.dataProcessingJobType)

    json.writeFieldName("errorsSinceLastSuccess")
    json.writeNumber(baseJob.errorsSinceLastSuccess)

    json.writeFieldName("uris")
    json.writeStartArray()
    baseJob.uris.foreach(json.writeString)
    json.writeEndArray()

    json.writeFieldName("environmentVariables")
    json.writeStartArray()
    baseJob.environmentVariables.foreach { v =>
      json.writeStartObject()
      json.writeFieldName("name")
      json.writeString(v.name)
      json.writeFieldName("value")
      json.writeString(v.value)
      json.writeEndObject()
    }
    json.writeEndArray()

    json.writeFieldName("arguments")
    json.writeStartArray()
    baseJob.arguments.foreach(json.writeString)
    json.writeEndArray()

    json.writeFieldName("highPriority")
    json.writeBoolean(baseJob.highPriority)

    json.writeFieldName("runAsUser")
    json.writeString(baseJob.runAsUser)

    if (baseJob.container != null) {
      json.writeFieldName("container")
      json.writeStartObject()
      // TODO: Handle more container types when added.
      json.writeFieldName("type")
      json.writeString("docker")
      json.writeFieldName("image")
      json.writeString(baseJob.container.image)
      json.writeFieldName("network")
      json.writeString(baseJob.container.network.toString)
      json.writeFieldName("volumes")
      json.writeStartArray()
      baseJob.container.volumes.foreach { v =>
        json.writeStartObject()
        v.hostPath.foreach { hostPath =>
          json.writeFieldName("hostPath")
          json.writeString(hostPath)
        }
        json.writeFieldName("containerPath")
        json.writeString(v.containerPath)
        v.mode.foreach { mode =>
          json.writeFieldName("mode")
          json.writeString(mode.toString)
        }
        json.writeEndObject()
      }
      json.writeEndArray()
      json.writeFieldName("forcePullImage")
      json.writeBoolean(baseJob.container.forcePullImage)

      json.writeFieldName("parameters")
      json.writeStartArray()
      baseJob.container.parameters.foreach { v =>
        json.writeStartObject()
        json.writeFieldName("key")
        json.writeString(v.key)
        json.writeFieldName("value")
        json.writeString(v.value)
        json.writeEndObject()
      }
      json.writeEndArray()

      json.writeEndObject()
    }

    json.writeFieldName("constraints")
    json.writeStartArray()
    baseJob.constraints.foreach { v =>
      json.writeStartArray()
      v match {
        case EqualsConstraint(attribute, value) =>
          json.writeString(attribute)
          json.writeString(EqualsConstraint.OPERATOR)
          json.writeString(value)
        case LikeConstraint(attribute, value) =>
          json.writeString(attribute)
          json.writeString(LikeConstraint.OPERATOR)
          json.writeString(value)
      }
      json.writeEndArray()
    }
    json.writeEndArray()

    baseJob match {
      case depJob: DependencyBasedJob =>
        json.writeFieldName("parents")
        json.writeStartArray()
        depJob.parents.foreach(json.writeString)
        json.writeEndArray()
      case schedJob: ScheduleBasedJob =>
        json.writeFieldName("schedule")
        json.writeString(schedJob.schedule)
        json.writeFieldName("scheduleType")
        json.writeString(schedJob.scheduleType.name)
        json.writeFieldName("scheduleTimeZone")
        json.writeString(schedJob.scheduleTimeZone)
      case iSchedJob: InternalScheduleBasedJob =>
        json.writeFieldName("scheduleTimeZone")
        json.writeString(iSchedJob.scheduleTimeZone)

        json.writeFieldName("scheduleType")
        json.writeString(iSchedJob.scheduleData.scheduleType.name)

        json.writeFieldName("scheduleData")
        json.writeStartObject()

        iSchedJob.scheduleData match {
          case CronSchedule(schedule, scheduleTimeZone, invocationTime, lastExecutionTime, cron) =>
            json.writeFieldName("schedule")
            json.writeString(schedule)
            json.writeFieldName("scheduleTimeZone")
            json.writeString(scheduleTimeZone)
            json.writeFieldName("invocationTime")
            json.writeString(invocationTime.toString(ISODateTimeFormat.dateTime))
            json.writeFieldName("lastExecutionTime")
            json.writeString(lastExecutionTime.toString(ISODateTimeFormat.dateTime))

          case Iso8601Schedule(schedule, scheduleTimeZone, invocationTime, originTime, offset, recurrences, period) =>
            json.writeFieldName("schedule")
            json.writeString(schedule)
            json.writeFieldName("scheduleTimeZone")
            json.writeString(scheduleTimeZone)
            json.writeFieldName("originTime")
            json.writeString(originTime.toString(ISODateTimeFormat.dateTime))
            json.writeFieldName("invocationTime")
            json.writeString(invocationTime.toString(ISODateTimeFormat.dateTime))
            json.writeFieldName("offset")
            json.writeNumber(offset)

            iSchedJob.scheduleData.recurrences.foreach { r =>
              json.writeFieldName("recurrences")
              json.writeNumber(r)
            }

            json.writeFieldName("period")
            json.writeString(period.toString(ISOPeriodFormat.standard()))
        }

        json.writeEndObject()
    }

    json.writeEndObject()
  }
}

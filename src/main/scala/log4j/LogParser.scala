package log4j

import java.io.BufferedReader
import java.util.regex.Matcher

class LogParser {
  protected def process(bufferedReader: BufferedReader) {
    var eventMatcher: Matcher = null
    var exceptionMatcher: Matcher = null
    var line: String = null
    while ((line = bufferedReader.readLine()) != null) {
      for (i <- 1 until lineCount) {
        val thisLine = bufferedReader.readLine()
        if (thisLine != null) {
          line = line + newLine + thisLine
        }
      }
      eventMatcher = regexpPattern.matcher(line)
      if (line.trim() == "") {
        //continue
      }
      exceptionMatcher = exceptionPattern.matcher(line)
      if (eventMatcher.matches()) {
        val event = buildEvent()
        if (event != null) {
          if (passesExpression(event)) {
            doPost(event)
          }
        }
        currentMap.putAll(processEvent(eventMatcher.toMatchResult()))
      } else if (exceptionMatcher.matches()) {
        additionalLines.add(line)
      } else {
        if (appendNonMatches) {
          val lastTime = currentMap.get(TIMESTAMP).asInstanceOf[String]
          if (currentMap.size > 0) {
            val event = buildEvent()
            if (event != null) {
              if (passesExpression(event)) {
                doPost(event)
              }
            }
          }
          if (lastTime != null) {
            currentMap.put(TIMESTAMP, lastTime)
          }
          currentMap.put(MESSAGE, line)
        } else {
          additionalLines.add(line)
        }
      }
    }
    val event = buildEvent()
    if (event != null) {
      if (passesExpression(event)) {
        doPost(event)
      }
    }
  }
}

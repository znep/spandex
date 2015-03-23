package com.socrata.spandex

import com.socrata.datacoordinator.secondary.{Event => SecondaryEvent}
import com.socrata.soql.types.{SoQLValue, SoQLType}

package object secondary {
  type Event = SecondaryEvent[SoQLType, SoQLValue]
  type Events = Iterator[Event]
}

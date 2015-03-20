package com.socrata.spandex

import com.socrata.datacoordinator.secondary.Event
import com.socrata.soql.types.{SoQLValue, SoQLType}

package object secondary {
  type Events = Iterator[Event[SoQLType, SoQLValue]]
}

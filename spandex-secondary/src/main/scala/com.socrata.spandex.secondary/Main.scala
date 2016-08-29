package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.SecondaryWatcherApp

object Main extends App {
  SecondaryWatcherApp(new SpandexSecondary(_))
}

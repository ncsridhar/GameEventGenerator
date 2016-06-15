# GameEventGenerator
This project generates a mobile app game's event data, with a configurable no. of users. It pushes the generated data to a Kafka topic. 

This is an akka based implementation I created in order to generate the following game user events:

- Game events: START_GAME, EVENT_<#>, END_GAME
- no. of simultaneous users: configurable
- no. of seconds this runs for: configurable

I modeled it after wrk tool, except this is specific for a mobile app game data.

This tool can be used as is or extended\modified, but this can be a powerful help for generating data for scale testing and data pipeline testing purposes.


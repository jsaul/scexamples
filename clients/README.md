# SeisComP clients

SeisComP clients connect to the SeisComP messaging and usually retrieve objects via the messaging.

As an example, a minimal client just retrieving picks from the messaging is provided.

Some clients also work with waveforms, either by streaming continuous waveforms or by fetching specific time windows on demand.
Here we provide code skeletons for both: polling-pick-client and streaming-pick-client.
In the examples provided here, both clients listen to pick objects from the messaging.
For each pick, short time windows of a few minutes around the pick times are retrieved for all three components according to the configuration.
Either on demand (polling-pick-client) or from a buffered, continuous stream of waveforms.
Use cases for this could include phase (re)picking, amplitude measurements or waveform modelling, provided we can work with only those streams for which the automatic picker produced picks.
If also unpicked streams shall be retrieved, a different strategy is needed, like using arrival times predicted from an event location.

The example clients provided here are

- [**pick-client**](messaging/pick-client) a minimal client only *listening* for picks 
- [**pick-sender**](messaging/pick-sender) a minimal client demonstrating how picks are *sent* via messaging
- [**polling-pick-client**](messaging+waveforms/polling-pick-client) is a client listening for picks and retrieving in demand short waveform time windows around the pick times
- [**streaming-pick-client**](messaging+waveforms/streaming-pick-client) also listens for picks, but retrieves waveform time windows from a continuous stream that is buffered for some time



## pick-client

This is a minimalistic script to demonstrate how to connect to the SeisComP messaging and to receive and process messages.
It handles the very simple (nearly trivial) case of picks and amplitudes and the only thing is does is dump a few informations about the objects to stdout.
If you want to do something else with the objects (likely you do...), then adopt the doSomethingWith*() methods accordingly.

## pick-sender

A simple SeisComP Python script for importing picks from a non-SeisComP picker, e.g. an existing legacy picker.

Being only an example, the script simply reads "picks" produced by a dummy "picker" from standard input and sends it to a SeisComP messaging.
Option --test enables test mode.

In a real-world-interface, however, you likely have other, different information produced by the legacy picker.
The parse() function will have to be adapted accordingly.


## polling-pick-client

This client listens for picks and for each pick retrieves a time window of waveform data around the pick time.
At the time the pick is received, now all of the waveform data may be available on the server.
In fact in a real-time scenario most of the time the timewindow will extend into the future at the time the pick is received.
In consequence we need to wait for some time after the pick was received in order for the entire time window to become available upstream.

A use case for that could be a repicker module or a module that determines amplitudes in different frequency bands or time windows than the standard SeisComP.

The advantage of this client is simplicity.
Maintain a list of picks for which we want to retrieve waveforms, and in short intervals request the waveforms from the upstream server.
Once the entire requested data time window could be retrieved, we are done.
No buffering of waveforms is required and access to archive data can be achieved in a totally transparent way.
The simplicity comes at the cost of overhead to to often multiple attempts (polling) needed to retrieve the entire waveform data window.
Also at the upstream server the availability of the waveforms may be delayed, which also delays the processing of these data.
In time critical applications this will therefore not be the first choice.


## streaming-pick-client

The streaming pick client continuously retrieves the waveforms for all configured data streams, no matter if our client needs the data or not.
In total the amount of data retrieved and discarded will be quite large, but the required bandwidth is still not very high.
In order to retrieve data time windows around specific picks we need to watch the progress of the data acquisition and once all data are available in the local waveform buffer, we simply retrieve the waveforms from there.
The advantage of this is that due to the streaming of the data the availability is immediate with practically no delay.
The disadvantage may be the handling of old data, i.e. whenever we need to access waveforms not in our data buffer any more.
Also due to the buffering of potentially large time windows, the memory usage can be quite high.

# SeisComP clients

SeisComP clients connect to the SeisComP messaging and usually retrieve objects via the messaging.
Some clients also work with waveforms.
These can be divided into streaming clients and clients that retrieve only on demand short time windows.
Here we provide code skeletons for both.

Both clients listen to pick objects from the messaging.
For each pick, short time windows of a few minutes around the pick times are retrieved for all three components according to the configuration.
Use cases for this could be phase (re)picking, amplitude measurements or waveform modelling, provided we can work with only those streams for which the automatic picker produced picks.


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

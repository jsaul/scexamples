import sys
import time
import pathlib
import seiscomp.client
import seiscomp.datamodel
import seiscomp.io

import scstuff.util
import scstuff.inventory


# We need a more convenient config for that:
global_net_sta_blacklist = [
    # bad component orientations
    ("WA", "ZON"),
]


class RequestItem:
    pass


class StreamBufferApp(seiscomp.client.StreamApplication):

    def __init__(self, argc, argv):
        seiscomp.client.StreamApplication.__init__(self, argc, argv)
        self.setMessagingEnabled(False)
        self.setLoadInventoryEnabled(True)

        # We need the config to determine which streams are used for picking
        self.setLoadConfigModuleEnabled(True)

        self.setRecordInputHint(seiscomp.core.Record.SAVE_RAW)

        # Time for which we buffer the data in seconds
        self.buffer_length = 3600.

        # This is the time window that we request for each repick.
        # Depending on the use case this may be shorter (or longer)
        self.before_p = 120.
        self.after_p = 240.
        self.expire_after = 1800.

        # Requests by pick ID. Only one request per pick ID.
        self.request = dict()

        # Requests accessible by nslc.
        # At any point in time there may be more than one request pending
        # for the same nslc so each item in the dict is itself a list.
        self.request_by_nslc = dict()

        # This is the waveform buffer.
        # There is one dict per stream, with the stream's nslc used as key.
        # Each item is another dict with the components stored in separate lists.
        self.buffer = dict()
        self.end_time = dict()

        self.cleanup_interval = 120.
        self.next_cleanup = seiscomp.core.Time.GMT() + seiscomp.core.TimeSpan(self.cleanup_interval)

    def setBufferLength(self, seconds):
        self.buffer_length = seconds

    def handleRecord(self, rec):
        """
        Virtual record handler that we implement here to store
        the data in ring buffers.
        """

        # This hack is required in order to acquire the ownership
        # of the record and also increase the reference count by 1.
        rec = seiscomp.core.Record.Cast(rec)
        nslc = scstuff.util.nslc(rec)
        n, s, l, c = nslc
        if l == "":
            l = "--"
        c, comp = c[:-1], c[-1]
        nslc = n, s, l, c

        # Find the right buffer
        if nslc not in self.buffer:
            # create a buffer for this stream
            self.buffer[nslc] = dict()
        if comp not in self.buffer[nslc]:
            self.buffer[nslc][comp] = list()
        # Store record
        self.buffer[nslc][comp].append(rec)

        # Update end time for this stream
        if nslc not in self.end_time:
            self.end_time[nslc] = dict()
        if comp not in self.end_time[nslc]:
            self.end_time[nslc][comp] = rec.endTime()
        if rec.endTime() > self.end_time[nslc][comp]:
            self.end_time[nslc][comp] = rec.endTime()

        if nslc not in self.request_by_nslc:
            # Nothing to do
            return

        # See if a data request for this stream is complete
        for request_item in self.request_by_nslc[nslc]:
            finished = True
            for comp in self.end_time[nslc]:
                if self.end_time[nslc][comp] < request_item.end_time:
                    finished = False
                    break
            if not finished:
                continue

            request_item.finished = True
            request_item.data = dict()
            for comp in self.buffer[nslc]:
                request_item.data[comp] = [
                    r for r in self.buffer[nslc][comp]
                    if r.endTime()   >= request_item.start_time and \
                       r.startTime() <= request_item.end_time]

            self.processData(request_item)

            self.request_by_nslc[request_item.nslc].remove(request_item)
            del self.request[request_item.pick.publicID()]

        self.cleanup_all()

    def processData(self, request_item):
        seiscomp.logging.info("Working with " + request_item.pick.publicID())

    def cleanup_stream(self, nslc):
        end_time = None
        for comp in self.buffer[nslc]:
            t = self.buffer[nslc][comp][-1].endTime()
            if end_time is None or t < end_time:
                end_time = t
        start_time = end_time - seiscomp.core.TimeSpan(self.buffer_length)
        for comp in self.buffer[nslc]:
            buf = self.buffer[nslc][comp]
            self.buffer[nslc][comp] = [
                    r for r in buf
                    if r.endTime() > start_time ]

    def cleanup_all(self):
        """ Trim all the waveform buffers """
        now = seiscomp.core.Time.GMT()

        if now < self.next_cleanup:
            return

        for nslc in self.buffer:
            self.cleanup_stream(nslc)

        self.next_cleanup = now + seiscomp.core.TimeSpan(self.cleanup_interval)

    def init(self):
        if not super().init():
            return False
        
        self.inventory = seiscomp.client.Inventory.Instance().inventory()

        configModule = self.configModule()
        self.configuredStreams = \
            scstuff.util.configuredStreams(self.configModule(), self.name())

        now = seiscomp.core.Time.GMT()
        self.components = scstuff.inventory.streamComponents(
            self.inventory, now,
            net_sta_blacklist=global_net_sta_blacklist)

        # start acquisition one hour ago
        tstart = now + seiscomp.core.TimeSpan(-3600)

        self.recordStream().setTimeout(300)
        self.recordStream().setStartTime(tstart)

        for nslc in self.configuredStreams:
            if nslc not in self.components:
                seiscomp.logging.debug("skipping %s" % (str(nslc),))
                continue
            n, s, l, c = nslc
            for comp in self.components[nslc]:
                self.recordStream().addStream(n, s, "" if l == "--" else l, c+comp)

        return True

    def processPick(self, pick):
        seiscomp.logging.debug("pick %s" % (pick.publicID(),))
        pickID = pick.publicID()
        n, s, l, c = scstuff.util.nslc(pick.waveformID())
        nslc = (n, s, "--" if l=="" else l, c[:2])

        t0 = pick.time().value()
        t1 = t0 + seiscomp.core.TimeSpan(-self.before_p)
        t2 = t0 + seiscomp.core.TimeSpan(+self.after_p)
        if nslc not in self.components:
            # This may occur if a station was (1) blacklisted or (2) added
            # to the processing later on. Either way we skip this pick.
            return

        now = seiscomp.core.Time.GMT()
        request_item = RequestItem()
        request_item.expires = now + seiscomp.core.TimeSpan(self.expire_after)
        request_item.pick = pick
        request_item.nslc = nslc
        request_item.components = self.components[nslc]
        request_item.start_time = t1
        request_item.end_time = t2
        request_item.finished = False
        self.request[pickID] = request_item
        if nslc not in self.request_by_nslc:
            self.request_by_nslc[nslc] = list()
        self.request_by_nslc[nslc].append(request_item)

    def addObject(self, parentID, obj):
        # called if a new object is received
        pick = seiscomp.datamodel.Pick.Cast(obj)
        if pick:
            self.processPick(pick)


class WaveformDumperApp(StreamBufferApp):
    """
    Based on the StreamBufferApp, this class implements dumping of waveforms
    to numbered directories for each incoming pick, along with an XML file
    containing the information about the pick itself.
    """

    def __init__(self, argc, argv):
        super().__init__(argc, argv)
        self.setMessagingEnabled(True)
        self.addMessagingSubscription("PICK")

        self.request_item_count = 0

        self.export_d = None

    def createCommandLineDescription(self):
        super().createCommandLineDescription()

        self.commandline().addGroup("Config")
        self.commandline().addStringOption(
            "Config", "export-dir,d", "path of the export directory")

    def validateParameters(self):
        """
        Command-line parameters
        """
        if not super().validateParameters():
            return False

        try:
            self.export_d = self.commandline().optionString("export-dir")
        except RuntimeError:
            pass
        if self.export_d is not None:
            self.export_d = pathlib.Path(self.export_d).expanduser()

        return True

    def init(self):
        if not super().init():
            return False
        
        if self.export_d is not None and self.export_d.exists():
            try:
                # continue with the export directory numbering
                last = sorted(self.export_d.glob("0*"))[-1].name
                self.request_item_count = int(last)
            except IndexError:
                self.request_item_count = 0

        return True

    def processData(self, request_item):
        super().processData(request_item)

        if self.export_d is not None:
            overwrite = False

            self.request_item_count += 1
            path = self.export_d / ("%09d" % self.request_item_count)
            path.mkdir(parents=True, exist_ok=True)
            n, s, l, c = request_item.nslc
            basename = "%s.%s.%s.%s" % (n, s, "" if l=="--" else l, c)
            for comp in request_item.components:
                if not comp in request_item.data:
                    continue
                mseed_filename = path / (basename + comp + ".mseed")
                if mseed_filename.exists() and not overwrite:
                    continue
                with open(mseed_filename, "wb") as f:
                    for rec in request_item.data[comp]:
                        f.write(rec.raw().str())

            # Dump pick to XML
            xml_filename = str(path / "pick.xml")
            ep = seiscomp.datamodel.EventParameters()
            ep.add(request_item.pick)
            ar = seiscomp.io.XMLArchive()
            ar.setFormattedOutput(True)
            ar.create(xml_filename)
            ar.writeObject(ep)
            ar.close()

        # Count items still in the request queue
        count_1 = len(self.request)
        count_2 = 0
        for nslc in self.request_by_nslc:
            count_2 += len(self.request_by_nslc[nslc])
        assert count_1 == count_2
        seiscomp.logging.debug("Pending %d items" % (count_1,))


def main():
    app = WaveformDumperApp(len(sys.argv), sys.argv)
    app()


if __name__ == "__main__":
    main()

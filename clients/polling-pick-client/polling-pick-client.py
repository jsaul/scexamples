import sys
import time
import seiscomp.client
import seiscomp.datamodel

import scdlpicker.util as _util
import scdlpicker.inventory as _inventory

# The acquisition will wait that long to finalize the acquisition
# of waveform time windows. The processing may be interrupted that
# long!
stream_timeout = 5

# Normally no need to change this
timeout_interval = 30

# We need a more convenient config for that:
global_net_sta_blacklist = [
    # bad components
    ("WA", "ZON"),
]

class RequestItem:
    pass


class App(seiscomp.client.Application):

    def __init__(self, argc, argv):
        seiscomp.client.Application.__init__(self, argc, argv)
        self.setMessagingEnabled(True)
        self.setLoadInventoryEnabled(True)
        self.setRecordStreamEnabled(True)
        self.addMessagingSubscription("PICK")

        # This is the time window that we request for each repick.
        # Depending on the use case this may be shorter (or longer)
        self.beforeP = 120.
        self.afterP = 240.
        self.expire_after = 1800.

        self.request = dict()

    def init(self):
        if not super(App, self).init():
            return False
        
        self.inventory = seiscomp.client.Inventory.Instance().inventory()

        now = seiscomp.core.Time.GMT()
        self.components = _inventory.streamComponents(
            self.inventory, now,
            net_sta_blacklist=global_net_sta_blacklist)

        return True


    def processPendingPicks(self):
        now = seiscomp.core.Time.GMT()
        def pt(p):
            return p.time().value()

        items_due = [i for i in self.request.values() if float(now - pt(i.pick)) > self.afterP]
        seiscomp.logging.debug("picks due %d" % (len(items_due),))

        # This is a brute-force request: Try and see what we get.
        #
        # If the requested data is not complete yet, the request will be
        # fully repeated after a certain time interval until we either get
        # the full time window of data or the request expired.
        t_begin_request = time.time()
        seiscomp.logging.info("Opening RecordStream "+self.recordStreamURL())
        stream = seiscomp.io.RecordStream.Open(self.recordStreamURL())
        stream.setTimeout(stream_timeout)
        stream_count = 0

        for item in items_due:
            n, s, l, c = item.nslc
            t1 = item.start_time
            t2 = item.end_time
            for comp in item.components:
                stream.addStream(n, s, "" if l == "--" else l, c+comp, t1, t2)
                stream_count += 1

        waveforms = dict()
        end_time = dict()

        seiscomp.logging.info(
            "RecordStream: requesting %d streams" % stream_count)
        count = 0
        for rec in _util.RecordIterator(stream, showprogress=True):
            if rec is None:
                break
            n = rec.networkCode()
            s = rec.stationCode()
            l = rec.locationCode()
            c = rec.channelCode()
            # "raw" nslc
            nslc = n, s, l, c
            if nslc not in waveforms:
                waveforms[nslc] = []
            waveforms[nslc].append(rec)

            c, comp = c[:-1], c[-1]
            nslc = n, s, "--" if l=="" else l, c
            if nslc not in end_time:
                end_time[nslc] = dict()
            if comp not in end_time[nslc]:
                end_time[nslc][comp] = rec.endTime()
            if rec.endTime() > end_time[nslc][comp]:
                    end_time[nslc][comp] = rec.endTime()
            count += 1

        seiscomp.logging.debug(
            "RecordStream: received %d records" % (count,))
        t_end_request = time.time()
        dt = t_end_request - t_begin_request
        seiscomp.logging.debug(
            "RecordStream: request lasted %.3f seconds" % (dt))

        finished_items = []
        for pickID in self.request:
            item = self.request[pickID]
            finished = True
            if item.nslc in end_time:
                for comp in item.components:
                    if item.nslc not in end_time:
                        # No record received (yet) for requested stream
                        finished = False
                        break
                    if comp not in end_time[item.nslc]:
                        # No record received (yet) for requested component
                        finished = False
                        break
                    if end_time[item.nslc][comp] < item.end_time:
                        # if *any* of the components is unfinished
                        finished = False
                        break
                if finished:
                    item.finished = True

            if item.finished:
                finished_items.append(item)

        for item in finished_items:
            pickID = item.pick.publicID()
            seiscomp.logging.debug("%s finished" % (pickID,))
            del self.request[pickID]

        expired_items = [i for i in self.request.values() if now > i.expires]
        for item in expired_items:
            pickID = item.pick.publicID()
            seiscomp.logging.debug("%s expired" % (pickID,))
            for comp in item.components:
                if comp not in end_time[item.nslc]:
                    seiscomp.logging.debug("  %s no data" % (comp,))
                    continue
                t2 = end_time[item.nslc][comp]
                seiscomp.logging.debug("  %s %s" % (comp, _util.isotimestamp(t2)))
            del self.request[pickID]

        seiscomp.logging.debug("%d pending request items" % (len(self.request)))

    def processPick(self, pick):
        seiscomp.logging.debug("pick %s" % (pick.publicID(),))
        pickID = pick.publicID()
        wfid = pick.waveformID()
        n = wfid.networkCode()
        s = wfid.stationCode()
        l = wfid.locationCode()
        c = wfid.channelCode()

        nslc = (n, s, "--" if l=="" else l, c[:2])

        t0 = pick.time().value()
        t1 = t0 + seiscomp.core.TimeSpan(-self.beforeP)
        t2 = t0 + seiscomp.core.TimeSpan(+self.afterP)
        if nslc not in self.components:
            # This may occur if a station was (1) blacklisted or (2) added
            # to the processing later on. Either way we skip this pick.
            return

        now = seiscomp.core.Time.GMT()
        item = RequestItem()
        item.expires = now + seiscomp.core.TimeSpan(self.expire_after)
        item.pick = pick
        item.nslc = nslc
        item.components = self.components[nslc]
        item.start_time = t1
        item.end_time = t2
        item.finished = False
        self.request[pickID] = item

    def handleTimeout(self):
        # The timeout interval can be configured via timeout_interval
        self.processPendingPicks()

    def addObject(self, parentID, obj):
        # called if a new object is received
        pick = seiscomp.datamodel.Pick.Cast(obj)
        if pick:
            self.processPick(pick)

    def run(self):
        self.enableTimer(timeout_interval)
        return super(App, self).run()

def main():
    app = App(len(sys.argv), sys.argv)
    app()


if __name__ == "__main__":
    main()

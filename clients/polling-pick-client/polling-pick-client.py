import sys
import time
import seiscomp.client
import seiscomp.datamodel

import scstuff.util
import scstuff.inventory


# The acquisition will wait that long to finalize the acquisition
# of waveform time windows. The processing may be interrupted that
# long!
stream_timeout = 5

# Normally no need to change this
timeout_interval = 30

# We need a more convenient config for that:
global_net_sta_blacklist = [
    # bad component orientations
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
        self.before_p = 120.
        self.after_p = 240.
        self.expire_after = 1800.

        self.request = dict()

    def init(self):
        if not super().init():
            return False
        
        self.inventory = seiscomp.client.Inventory.Instance().inventory()

        now = seiscomp.core.Time.GMT()
        self.components = scstuff.inventory.streamComponents(
            self.inventory, now,
            net_sta_blacklist=global_net_sta_blacklist)

        return True


    def processPendingPicks(self):
        now = seiscomp.core.Time.GMT()
        def pt(p):
            return p.time().value()

        request_items_due = [
                i for i in self.request.values()
                if float(now - pt(i.pick)) > self.after_p ]
        seiscomp.logging.debug("picks due %d" % (len(request_items_due),))

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

        for request_item in request_items_due:
            n, s, l, c = request_item.nslc
            t1 = request_item.start_time
            t2 = request_item.end_time
            for comp in request_item.components:
                stream.addStream(n, s, "" if l == "--" else l, c+comp, t1, t2)
                stream_count += 1

        waveforms = dict()
        end_time = dict()

        seiscomp.logging.info(
            "RecordStream: requesting %d streams" % stream_count)
        count = 0
        for rec in scstuff.util.RecordIterator(stream, showprogress=True):
            if rec is None:
                break
            nslc = scstuff.util.nslc(rec)
            # "raw" nslc
            if nslc not in waveforms:
                waveforms[nslc] = []
            waveforms[nslc].append(rec)

            n, s, l, c = nslc
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

        finished_request_items = []
        for pickID in self.request:
            request_item = self.request[pickID]
            finished = True
            if request_item.nslc in end_time:
                for comp in request_item.components:
                    if request_item.nslc not in end_time:
                        # No record received (yet) for requested stream
                        finished = False
                        break
                    if comp not in end_time[request_item.nslc]:
                        # No record received (yet) for requested component
                        finished = False
                        break
                    if end_time[request_item.nslc][comp] < request_item.end_time:
                        # if *any* of the components is unfinished
                        finished = False
                        break
                if finished:
                    request_item.finished = True

            if request_item.finished:
                finished_request_items.append(request_item)

        for request_item in finished_request_items:
            pickID = request_item.pick.publicID()
            seiscomp.logging.debug("%s finished" % (pickID,))
            del self.request[pickID]

        expired_request_items = [
            i for i in self.request.values()
            if now > i.expires]
        for request_item in expired_request_items:
            pickID = request_item.pick.publicID()
            seiscomp.logging.debug("%s expired" % (pickID,))
            for comp in request_item.components:
                if comp not in end_time[request_item.nslc]:
                    seiscomp.logging.debug("  %s no data" % (comp,))
                    continue
                t2 = end_time[request_item.nslc][comp]
                t2 = scstuff.util.isotimestamp(t2)
                seiscomp.logging.debug("  %s %s" % (comp, t2))
            del self.request[pickID]

        seiscomp.logging.debug("%d pending request items" % (len(self.request)))

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
        return super().run()


def main():
    app = App(len(sys.argv), sys.argv)
    app()


if __name__ == "__main__":
    main()

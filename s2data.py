import ddosa
import os
import glob
import re
import pyfits
import eddosa

class S2Events(ddosa.DataAnalysis):
    input_rev=ddosa.Revolution
    input_timestamp=""

    def main(self):
        self.eventfilepath=self.input_rev.revdir+"/raw/isgri_raw_cal_%s_00.fits.gz"%self.input_timestamp.str()
        if not os.path.exists(self.eventfilepath):
            return 

class S2EventsRev(ddosa.DataAnalysis):
    input_rev=ddosa.Revolution

    copy_cached_input=False
    run_for_hashe=False
    allow_alias=True

    def main(self):
        r=[]
        for fn in glob.glob(self.input_rev.revdir+"/raw/isgri_raw_cal_*_00.fits.gz"):
            ts=re.search("isgri_raw_cal_(.*?)_00.fits",fn).group(1)
            r.append(S2Events(input_timestamp=ts))
        self.thelist=r
    

class ScWDataFixed(ddosa.ScWData):
    pass


class EventsFromS2(ddosa.DataAnalysis):
    input_S2Events=S2Events

    cached=True

    def main(self):
        f=pyfits.open(self.input_S2Events.eventfilepath)

        fn="isgri_s2_events.fits"
        dc=ddosa.heatool("dal_create")
        dc['obj_name']="!"+fn
        dc['template']="ISGR-EVTS-ALL.tpl"
        dc.run()
        fo=pyfits.open(fn)
        
        
        fo['ISGR-EVTS-ALL'] = pyfits.BinTableHDU.from_columns(fo['ISGR-EVTS-ALL'].columns, nrows=f['ISGR-CDTE-CRW'].data.shape[0],header=fo['ISGR-EVTS-ALL'].header)

        fo['ISGR-EVTS-ALL'].data['ISGRI_PHA']=f['ISGR-CDTE-CRW'].data['ISGRI_PHA']
        fo['ISGR-EVTS-ALL'].data['RISE_TIME']=f['ISGR-CDTE-CRW'].data['RISE_TIME']
        fo['ISGR-EVTS-ALL'].data['ISGRI_Y']=f['ISGR-CDTE-CRW'].data['ISGRI_Y']
        fo['ISGR-EVTS-ALL'].data['ISGRI_Z']=f['ISGR-CDTE-CRW'].data['ISGRI_Z']

        fo.writeto(fn,clobber=True)
        self.evts=ddosa.DataFile(fn)

class ScWData(ddosa.DataAnalysis):
    input_s2=S2Events
    input_alls2=EventsFromS2

    #allow_alias=False
    #run_for_hashe=True

    copy_cached_input=False
    cached=True

    def main(self):
        ts=self.input_s2.input_timestamp.str()
        utc=ts[:4]+"-"+ts[4:6]+"-"+ts[6:8]+"T"+ts[8:10]+":"+ts[10:12]+":"+ts[12:14]
        print utc
        d=ddosa.fromUTC(utc)
        print d
        return ScWDataFixed(input_scwid=d['SCWID']+".001",use_isgrievents=self.input_alls2.evts)

class ibis_isgr_energy(eddosa.ibis_isgr_energy):
    input_eventfile=EventsFromS2
    copy_cached_input=False

    cached=True


import dataanalysis
cache_local=dataanalysis.MemCache()


class ISGRIEvents(ddosa.ISGRIEvents):
    cached=True
    cache=cache_local

    input_evttag=ibis_isgr_energy

    read_caches=[cache_local.__class__]
    write_caches=[cache_local.__class__]

class ibis_isgr_energy_S2Rev(ddosa.DataAnalysis):
    input_s2e=S2EventsRev
    copy_cached_input=False

    input_will_use_promise=eddosa.BasicEventProcessingSummary

    allow_alias=True
    
    def main(self):
        self.thelist=[ISGRIEvents(assume=s) for s in self.input_s2e]
        

class BinBackgroundRev(eddosa.BinBackgroundRevP2):
    copy_cached_input=False
    input_eventfiles=ibis_isgr_energy_S2Rev

import dataanalysis as da

class BinBackgroundList(da.DataAnalysis):
    input_eventfiles=S2EventsRev #ibis_isgr_energy_S2Rev
    #input_eventfiles=ibis_isgr_energy_S2Rev

    copy_cached_input=False
    allow_alias=True
    input_will_use_promise=eddosa.BasicEventProcessingSummary

    def main(self):
        self.thelist=[]
        for s in self.input_eventfiles.thelist:
            a=eddosa.BinBackgroundSpectrum(assume=s)
            print a,a.assumptions
            self.thelist.append(a)


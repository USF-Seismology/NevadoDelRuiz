#!/usr/bin/env python
# coding: utf-8

# Convert OVSM data from 2-minute long MiniSEED files in a YYYY/MM/DD directory tree to a SeisComP Data Structure (SDS)
# SDS directory tree structure / file naming convention is one MiniSEED file per SEED ID per day
# https://www.seiscomp.de/seiscomp3/doc/applications/slarchive/SDS.html
# 
# Source data:
#     from Google Drive folder https://drive.google.com/drive/folders/1DWcdFgYl-nmuNAi4TWr4Vqexv6xfcFEh
#     20240523-Datos_Ruiz_2012/ from Carlos Cardona via an email from Yuly. Download each month 04.zip, 05.zip, 06.zip as a zip file 
#     There are also SUDS_multiplexados_1 and _2 folders, 
#     from OVSM (Manizales observatory)
#
# Author: Glenn Thompson 2024/12/08
#
import os
import glob
import obspy

homedir = os.path.expanduser('~')
datatop = os.path.join(homedir, 'Desktop', 'DATA')
datadir = os.path.join(datatop, 'NevadoDelRuiz','suds')#,'2012', '04', '04')

# Input data extracted to this directory tree structure
YYYY = '2012'
mseeddir = os.path.join(datadir, YYYY)

# network suggested by Felix
network = 'NR'

# Top of SDS 
SDSdir = os.path.join(datatop, 'NevadoDelRuiz', 'SDS')

def fix_seedid(st, network='NR'):
    for tr in st: # change the network
        if tr.stats.station=='IRIG':
            st.remove(tr)
            continue
        old_id = tr.id
        tr.stats.network = network
        if tr.stats.station[-1] in 'ZNE' and len(st.select(station=f'{tr.stats.station[0:3]}*'))>1:
            tr.stats.channel = 'EH' + tr.stats.station[-1]    
            tr.stats.station = tr.stats.station[0:3]
        else:
            tr.stats.channel='EHZ'
        if len(tr.stats.station)==4:
            if tr.stats.station[-1]=='L' and len(st.select(station=f'{tr.stats.station[0:3]}*'))>1:
                tr.stats.channel = 'EL' + tr.stats.channel[2]    
                tr.stats.station = tr.stats.station[0:3]
            if tr.stats.station[-1]=='H' and len(st.select(station=f'{tr.stats.station[0:3]}*'))>1: 
                tr.stats.station = tr.stats.station[0:3]  

def append_and_merge(dayst, st):
    for tr in st:
        dayst.append(tr.copy())
    try:
        dayst.merge(method=0, fill_value=0)
    except:
        for tr in dayst:
            tr.stats.sampling_rate = float(round(tr.stats.sampling_rate))
        dayst.merge(method=0, fill_value=0) 
        
def write_day_to_SDS(dayst, SDSdir):
    for tr in dayst:
        startt = tr.stats.starttime
        sdsfulldir = os.path.join(SDSdir, startt.strftime('%Y'), tr.stats.network, tr.stats.station, tr.stats.channel+'.D')
        sdsfullpath = os.path.join(sdsfulldir, f"{tr.id}.D.{startt.strftime('%Y.%j')}")
        if not os.path.isdir(sdsfulldir):
            os.makedirs(sdsfulldir) 
        print(f'Writing {sdsfullpath}')
        tr.write(sdsfullpath, format='MSEED')  
                 
for monthdir in sorted(glob.glob(os.path.join(mseeddir, '[0-1][0-9]'))):
    #for daydir in sorted(glob.glob(os.path.join(monthdir, '[0-3][0-9]'))):
    for daydir in sorted(glob.glob(os.path.join(monthdir, '[2-3][0-9]'))):    
        if os.path.isdir(daydir):
            print('\n************************')
            print(daydir)
            dayst = obspy.Stream()
            lastday = None
            for filepath in sorted(glob.glob(os.path.join(daydir, '*.mseed'))):
                if os.path.isfile(filepath):
                    print(f'Reading {filepath}')                    
                    try:
                        st = obspy.read(filepath, format='MSEED')
                    except Exception as e: 
                        print('\nUnknown format for ',filepath)
                    else:
                        fix_seedid(st, network=network)              
                        currentday = st[0].stats.starttime.day
                        currentendday = st[0].stats.endtime.day
                        if currentday != currentendday: # current Stream crosses a day boundary
                            # split
                            nextst = st.copy().trim(starttime=dayetime)
                            st.trim(endtime=dayetime) # this is what we append
                            append_and_merge(dayst, st) 
                            st = nextst
                            currentday = currentendday # force a write in next section
                                                       
                        if currentday != lastday: # new day
                            if len(dayst)>0: # write out previous day
                                write_day_to_SDS(dayst, SDSdir)

                            dayst = st.copy() # we wrote out last day, so reset to first Stream for this day
                            trstime = dayst[0].stats.starttime
                            daystime = obspy.UTCDateTime(trstime.year, trstime.month, trstime.day)
                            dayetime = daystime + 86400
                            lastday = currentday
                            
                        else:
                            append_and_merge(dayst, st)
                            print(dayst[0])
                                    
import numpy as np
import pandas as p
# import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
import sys
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("resultsFile", type=str, help="Results file")
args=parser.parse_args()

def bldTitle(fn, jtitle, jfooter):

    title = "%s runTime %ds memQuota %dMb keyLength %d valueLength %d keyOrder %s\n" % ( jtitle['runDescription'], jtitle['runTime'], jtitle['memQuota']/1024/1024, jtitle['keyLength'], jtitle['valueLength'], jtitle['keyOrder'])

    title += "Writers # %d BatchSize %d ThinkTime %dms   Readers # %d BatchSize %d ThinkTime %dms\n" % (jtitle['numWriters'], jtitle['writeBatchSize'], jtitle['writeBatchThinkTime']*1000, jtitle['numReaders'], jtitle['readBatchSize'], jtitle['readBatchThinkTime']*1000)

    title += "Compaction Percentage %0.2f LevelMaxSegs %d LevelMultipler %d BufferPages %d" % (jtitle['cfg_CompactionPercentage'], jtitle['cfg_CompactionLevelMaxSegments'], jtitle['cfg_CompactionLevelMultiplier'], jtitle['cfg_CompactionBufferPages'])

    return title

def bldFooter(jfooter, jtitle, d):
    totalWriteBytes = jfooter['tot_write_sectors'] * 512
    totalReadBytes = jfooter['tot_read_sectors'] * 512
    totalDbGrowth = (jtitle['keyLength']+jtitle['valueLength']) * jfooter['tot_numKeysWrite']

    if totalDbGrowth > 0:
        writeAmp = float(totalWriteBytes)/totalDbGrowth
        readAmp = float(totalReadBytes)/totalDbGrowth
    else:
        writeAmp = 0
        readAmp = 0

    footer = "Disk I/O    # Writes %d  %.1fGb  Amp %.1f    " % (jfooter['tot_write_ios'], totalWriteBytes/1024/1024/1024, writeAmp)

    footer += "# Reads %d  %.1fGb  Amp %.1f\n" % (jfooter['tot_read_ios'], float(totalReadBytes)/1024/1024/1024, readAmp)

    if int(jtitle['runTime']) > 0:
        avgKeyWrite = int(jfooter['tot_numKeysWrite']) / int(jtitle['runTime'])
        avgBatchWrite = int(jfooter['tot_numWriteBatches']) / int(jtitle['runTime'])
        avgKeyRead = int(jfooter['tot_numKeysRead']) / int(jtitle['runTime'])
        avgBatchRead = int(jfooter['tot_numReadBatches']) / int(jtitle['runTime'])
    else:
        avgKeyWrite = 0
        avgKeyRead = 0
        avgBatchWrite = 0
        avgBatchRead = 0

    footer += "Key Writes    # Keys %d  Avg/s %d   Batches %d  Avg/s %d\n" % (jfooter['tot_numKeysWrite'], avgKeyWrite, jfooter['tot_numWriteBatches'], avgBatchWrite)

    footer += "Key Reads     # Keys %d  Avg/s %d   Queries %d  Avg/s %d\n" % (jfooter['tot_numKeysRead'], avgKeyRead, jfooter['tot_numReadBatches'], avgBatchRead)

    footer += "# Persists %d   # Compactions %d   # Blocks %d   BlockTime %.1fs\n" % (jfooter['tot_total_persists'], jfooter['tot_total_compactions'], jfooter['tot_mhBlocks'], jfooter['tot_mhBlockDuration']/1000000)

    return footer

def sdl():
    return

def setMinVal(v, d):
    if d[v]['min'] < d[v]['0.01%'] - d[v]['std']:
        mv = d[v]['0.01%']
    else:
        mv = d[v]['min']
    return mv

def setMaxVal(v, d):
    if d[v]['max'] > d[v]['99.9%'] + d[v]['std']:
        mx = d[v]['99.9%']
    else:
        mx = d[v]['max']
    return mx

def main():
    fileName = args.resultsFile
    fn = fileName.split(".")[0]
    fh = open(fileName, "r")
    lines = fh.readlines()
    jtitle = json.loads(lines[0])
    jfooter = json.loads(lines[len(lines)-1])

    try:
        resultsFile = p.read_json(fileName, orient='records', lines=True)
    except ValueError as e:
        print "oops %s" %(e)
        return

    # convert bytes to MB
    #
    resultsFile['memused'] = (resultsFile['memtotal'] - (resultsFile['memfree']))/1024/1024
    resultsFile['cached'] = resultsFile['cached']/1024/1024
    resultsFile['mapped'] = resultsFile['mapped']/1024/1024
    resultsFile['processMem'] = resultsFile['processMem']/1024/1024
    resultsFile['mossSize'] = resultsFile['num_bytes_used_disk']/1024/1024
    resultsFile['dbSize'] = (resultsFile['totalKeyBytes'] + resultsFile['totalValBytes'])/1024/1024

    #
    # delta_ms is the time slice between samples in ms
    #
    resultsFile['delta_ms'] = (resultsFile['cpu_user'] + resultsFile['cpu_idle'] + resultsFile['cpu_iowait'] + resultsFile['cpu_system']) * 1000 / jtitle['ncpus'] / 100

    resultsFile['numKeysRead'] = resultsFile['numKeysRead']*1000/resultsFile['delta_ms']
    resultsFile['numKeysWrite'] = resultsFile['numKeysWrite']*1000/resultsFile['delta_ms']

    resultsFile['read_ios'] = resultsFile['read_ios']*1000/resultsFile['delta_ms']
    resultsFile['write_ios'] = resultsFile['write_ios']*1000/resultsFile['delta_ms']

    resultsFile['read_mbs'] = (resultsFile['read_sectors'] * 512 * 1000 / resultsFile['delta_ms'])/1024/1024
    resultsFile['write_mbs'] = (resultsFile['write_sectors'] * 512 * 1000 / resultsFile['delta_ms'])/1024/1024

    resultsFile['queuelen'] = resultsFile['avq']/resultsFile['delta_ms']
    resultsFile['iops'] = ((resultsFile['read_ios'] + resultsFile['write_ios'])*1000)/resultsFile['delta_ms']

    d = resultsFile.describe(percentiles=[.0001, .999])

    title = bldTitle(fn, jtitle, jfooter)
    footer = bldFooter(jfooter, jtitle, d)

    sdl()

    fig = Figure(figsize=(10,12))

    #
    # key reads/writes
    #
    ax = fig.add_subplot(911)
    minval = setMinVal('numKeysRead', d)
    maxval = setMaxVal('numKeysRead', d)
    y1 = np.array(resultsFile['numKeysRead'])
    x1 = np.arange(1, y1.size+1)
    ax.plot(x1, y1, label='Key Reads', color='red', alpha=0.7)
    ax.set_ylabel('Key Reads', color='red')
    ax.set_ylim([minval, maxval])
    ax.set_xticks([])

    ax2 = ax.twinx()
    minval = setMinVal('numKeysRead', d)
    maxval = setMaxVal('numKeysRead', d)
    y1 = np.array(resultsFile['numKeysWrite'])
    x1 = np.arange(1, y1.size+1)
    ax2.plot(x1, y1, label='Key Writes', color='blue', alpha=0.7)
    ax2.set_ylabel('Key Writes', color='blue')
    ax2.set_ylim([minval, maxval])
    ax2.set_xticks([])

    #
    # disk reads/writes
    #
    ax = fig.add_subplot(912)
    minval = setMinVal('read_ios', d)
    maxval = setMaxVal('read_ios', d)
    y1 = np.array(resultsFile['read_ios'])
    x1 = np.arange(1, y1.size+1)
    ax.plot(x1, y1, label='Disk Reads', color='red', alpha=0.7)
    ax.set_ylabel('Disk Reads', color='red')
    ax.set_ylim([minval, maxval])
    ax.set_xticks([])

    ax2 = ax.twinx()
    minval = setMinVal('write_ios', d)
    maxval = setMaxVal('write_ios', d)
    y1 = np.array(resultsFile['write_ios'])
    x1 = np.arange(1, y1.size+1)
    ax2.plot(x1, y1, label='Disk Writes', color='blue', alpha=0.7)
    ax2.set_ylabel('Disk Writes', color='blue')
    ax2.set_ylim([minval, maxval])
    ax2.set_xticks([])

    #
    # Read/Write Mb/s
    #
    ax = fig.add_subplot(913)
    minval = setMinVal('read_mbs', d)
    maxval = setMaxVal('read_mbs', d)
    y1 = np.array(resultsFile['read_mbs'])
    x1 = np.arange(1, y1.size+1)
    ax.plot(x1, y1, label='Read Mb/s', color='red', alpha=0.7)
    ax.set_ylabel('Read Mb/s', color='red')
    ax.set_ylim([minval, maxval])
    ax.set_xticks([])

    ax2 = ax.twinx()
    minval = setMinVal('write_mbs', d)
    maxval = setMaxVal('write_mbs', d)
    y1 = np.array(resultsFile['write_mbs'])
    x1 = np.arange(1, y1.size+1)
    ax2.plot(x1, y1, label='Write Mb/s', color='blue', alpha=0.7)
    ax2.set_ylabel('Write Mb/s', color='blue')
    ax2.set_ylim([minval, maxval])
    ax2.set_xticks([])

    #
    # iops and ioq
    #
    ax = fig.add_subplot(914)
    minval = setMinVal('iops', d)
    maxval = setMaxVal('iops', d)
    y1 = np.array(resultsFile['iops'])
    x1 = np.arange(1, y1.size+1)
    ax.plot(x1, y1, label='iops', color='green', alpha=0.7)
    ax.set_ylabel('iops', color='green')
    ax.set_ylim([minval, maxval])
    ax.set_xticks([])

    ax2 = ax.twinx()
    minval = setMinVal('queuelen', d)
    maxval = setMaxVal('queuelen', d)
    y1 = np.array(resultsFile['queuelen'])
    x1 = np.arange(1, y1.size+1)
    ax2.plot(x1, y1, label='I/O Queue', color='yellow', alpha=0.7)
    ax2.set_ylabel('I/O Queue', color='yellow')
    ax2.set_ylim([minval, maxval])
    ax2.set_xticks([])

    #
    # memory
    #
    ax = fig.add_subplot(915)
    minval = min(d['memused']['min'], d['cached']['min'], d['mapped']['min'], d['processMem']['min'])
    maxval = max(d['memused']['max'], d['cached']['max'], d['mapped']['max'], d['processMem']['max'])
    y1 = np.array(resultsFile['memused'])
    y2 = np.array(resultsFile['cached'])
    y3 = np.array(resultsFile['mapped'])
    y4 = np.array(resultsFile['processMem'])
    x1 = np.arange(1, y1.size+1)
    ax.plot(x1, y1, label='Total Sys Memory', color='red', alpha=0.7)
    ax.plot(x1, y2, label='Total FS Cache', color='blue', alpha=0.7)
    ax.plot(x1, y3, label='Total MMap', color='green', alpha=0.7)
    ax.plot(x1, y4, label='Process Mem', color='yellow', alpha=0.7)
    ax.set_ylim([minval, maxval])
    ax.legend(loc='upper center', fontsize="small", bbox_to_anchor=(0.5, 1.20), ncol=4, fancybox=True)
    ax.set_xticks([])


    #
    # database size
    #
    ax = fig.add_subplot(916)
    minval = min(d['dbSize']['min'], d['mossSize']['min'])
    maxval = max(d['dbSize']['max'], d['mossSize']['max'])
    y1 = np.array(resultsFile['dbSize'])
    y2 = np.array(resultsFile['mossSize'])
    x1 = np.arange(1, y1.size+1)
    ax.plot(x1, y1, label='DB Size', color='red', alpha=0.7)
    ax.plot(x1, y2, label='Moss Size', color='blue', alpha=0.7)
    ax.legend(loc='upper center', fontsize="small", bbox_to_anchor=(0.5, 1.20), ncol=2, fancybox=True)
    ax.set_ylim([minval, maxval])
    ax.set_xticks([])

    #
    # cpu time
    #
    ax = fig.add_subplot(917)
    minval = min(setMinVal('cpu_user', d), setMinVal('cpu_system', d), setMinVal('cpu_idle', d), setMinVal('cpu_iowait', d))
    maxval = min(setMaxVal('cpu_user',d), setMaxVal('cpu_system', d), setMaxVal('cpu_idle', d), setMaxVal('cpu_iowait', d))
    y1 = np.array(resultsFile['cpu_user'])
    y2 = np.array(resultsFile['cpu_system'])
    y3 = np.array(resultsFile['cpu_iowait'])
    y4 = np.array(resultsFile['cpu_idle'])

    x1 = np.arange(1, y1.size+1)
    ax.plot(x1, y1, label='User', color='blue', alpha=0.7)
    ax.plot(x1, y2, label='System', color='red', alpha=0.7)
    ax.plot(x1, y3, label='IOwait', color='green', alpha=0.7)
    ax.plot(x1, y4, label='Idle', color='yellow', alpha=0.7)
    ax.legend(loc='upper center', fontsize="small", bbox_to_anchor=(0.5, 1.20), ncol=4, fancybox=True)
    ax.set_ylim([minval, maxval])
    ax.set_xticks([])

    #
    # moss statistics
    #
    ax = fig.add_subplot(918)
    minval = min(d['mhBlocks']['min'], d['total_persists']['min'], d['num_segments']['min'], d['num_files']['min'])
    maxval = max(d['mhBlocks']['max'], d['total_persists']['max'], d['num_segments']['max'], d['num_files']['max'])
    y1 = np.array(resultsFile['mhBlocks'])
    y2 = np.array(resultsFile['total_persists'])
    y3 = np.array(resultsFile['num_files'])
    y4 = np.array(resultsFile['num_segments'])

    x1 = np.arange(1, y1.size+1)
    ax.plot(x1, y1, label='Blocks', color='red', alpha=0.7)
    ax.plot(x1, y2, label='Persists', color='green', alpha=0.7)
    ax.plot(x1, y3, label='Files', color='magenta', alpha=0.7)
    ax.plot(x1, y4, label='Segments', color='black', alpha=0.7)
    ax.legend(loc='upper center', fontsize="small", bbox_to_anchor=(0.5, 1.20), ncol=4, fancybox=True)
    ax.set_ylim([minval, maxval])
    ax.set_xticks([])

    ax2 = ax.twinx()
    minval = 0
    maxval = d['total_compactions']['max']
    y1 = np.array(resultsFile['total_compactions'])
    x1 = np.arange(1, y1.size+1)
    ax2.plot(x1, y1, label='Compactions', color='blue', alpha=0.7)
    ax2.set_ylabel('Compactions', color='blue')
    ax2.set_ylim([minval, maxval])
    ax2.set_xticks([])


    fig.suptitle(title, fontsize=12)
    fig.text(0.1, 0, footer, fontsize=11)
    canvas = FigureCanvasAgg(fig)
    outFile = "%s.png" % (fn)
    canvas.print_figure(outFile, dpi=80)

main()

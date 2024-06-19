# File system monitoring
# Exception measures Contained
# Scan Contained

from subprocess import run
from os import scandir
import datetime
import threading
import time
import matplotlib.pyplot as plt
from matplotlib import use
import numpy as np

use('agg')

MonDir = '/home/kuldeep/MasterData'
LandDir = '/home/kuldeep/analysis/Output'

AnDir = '/home/kuldeep/analysis'  # Main
SingImgPath = f'{AnDir}/psrsoft_py3_new.sif'
DMcalcPath = f'{AnDir}/dmcalc.py'
ParPath = f'{AnDir}/parfile'
ModelPath = f'{AnDir}/templates'
DMCalc_logsPath = f'{AnDir}/DMCalc_logs.txt'


def monitor():
    cmd = f'while file=$(inotifywait -q -e create -t 10 --format "%w%f" -r "{MonDir}"); do echo "$file"; done'
    try:
        var = run(cmd, shell=True, capture_output=True, text=True)
        print(f'Monitoring {MonDir}', datetime.datetime.now())
    except:
        print("Couldn't start monitoring.", datetime.datetime.now())
        return 0

    if var.stdout != '':
        varstm = (var.stdout.rstrip()).split("\n")
        return varstm
    else:
        return 0


def scan():
    mfiles = []
    cmd = f'find {LandDir}/ -printf "%p\n" | grep "files_copied.txt"'
    records = run(cmd, shell=True, capture_output=True, text=True).stdout
    if records != '':
        recordsls = (records.rstrip()).split("\n")
        for recfile in recordsls:
            Cycle, Pulsar, Band, Name = recfile.split("/")[-4:]
            RecPath = f'{LandDir}/{Cycle}/{Pulsar}/{Band}/{Name}'
            rec = np.loadtxt(RecPath, usecols=(0,), dtype=str)
            MasDir = f'/home/kuldeep/MasterData/{Cycle}/{Pulsar}/{Band}'
            for i in scandir(f'{MasDir}/'):
                if i.path not in rec:
                    copy(i.path)
                    mfiles.append(i.path)
    return mfiles
                    
    
    
    
def copy(filename):
    Cycle,Pulsar,Band,Name = filename.split("/")[-4:]
    DestDir = f'{LandDir}/{Cycle}/{Pulsar}/{Band}'
    try:
        cmd2 = f'mkdir -p {DestDir}/; cp -d {filename} {DestDir}/ && echo {filename} >> {DestDir}/files_copied.txt'
        run(cmd2, shell=True)
        print(f'File {filename} copied succesfully.')
    except:
        print(f'Failed to move file {filename}')

def Time_residual_plot(OutputPath,Pulsar):
    fmt = '\'' + '! {bat} {pre} {err}\\n' + '\'' + '\"' + '\"'
    grep = "| grep '^!' | cut -c 3- >"
    cmd = f'cd {OutputPath} && singularity exec {SingImgPath} tempo2 -output general2 -f {ParPath}/{Pulsar}.par {OutputPath}/{Pulsar}_allToAs.tim -s {fmt} {grep} {OutputPath}/Residual_plot.txt'
    try:
        run(cmd, shell=True)
        
        temp = np.loadtxt(f"{OutputPath}/Residual_plot.txt")
        temp = np.transpose(temp)
        x = temp[0]
        y = temp[1]
        yerr = temp[2]*0.000001
        plt.errorbar(x,y,yerr = yerr, color='red', ls='', marker='o', capsize=5, capthick=1, ecolor='black')
        plt.xlabel("Time")
        plt.ylabel("Time residual (seconds)")
        plt.savefig(f'{OutputPath}/time_residual_timeseries.pdf', bbox_inches='tight')
        plt.close()
        print("Time residual plotted", datetime.datetime.now())
    except:
        print("Couldn't plot time residual.", datetime.datetime.now())
        
def DM_Time_Series_Plot(OutputPath,Pulsar):
    try:
        temp = np.loadtxt(f"{OutputPath}/{Pulsar}_DM_timeseries.txt", usecols=range(3))
        temp = np.transpose(temp)
        x = temp[0]
        y = temp[1]
        yerr = temp[2]
        plt.errorbar(x,y,yerr = yerr, color='blue', ls='', marker='o', capsize=5, capthick=1, ecolor='black')
        plt.xlabel("Time")
        plt.ylabel("DM value")
        plt.savefig(f'{OutputPath}/DM_timeseries.pdf', bbox_inches='tight')
        plt.close()
        print("DM timeseries plotted.", datetime.datetime.now())
    except:
        print("Couldn't plot DM timeseries.", datetime.datetime.now())
    

def analysis(files):
    for filename in files:
        print(f'{filename} Analysis started')
        Cycle,Pulsar,Band,Name = filename.split("/")[-4:]
        
        OutputPath = f'{LandDir}/{Cycle}/{Pulsar}/{Band}'
        TargetFilePath = f'{OutputPath}/{Name}'
        
        if Band == 'BAND3.200':
            MBand = '_b3_200.std'
        elif Band == 'BAND5.200':
            MBand = '_b5_200.std'
        else:
            print('NOT A VALID BAND')
            return 0
        
        n3cmd1 = f'cd {OutputPath}'
        n3cmd2 = f'singularity exec {SingImgPath} {DMcalcPath} -E {ParPath}/{Pulsar}.par -M {ModelPath}/{Pulsar}{MBand} -nch 16 {TargetFilePath} >> {DMCalc_logsPath}'
        n3cmd3 = f'rm {TargetFilePath}'
        n3cmd4 = f'(mkdir Exceptions ; mv {TargetFilePath} ./Exceptions/)'
        # Ensures whatever files raised error for dmcalc get stored in 'Exception' Folder.
        # When Duplicate named file is given to dmcalc, it too gets pushed into Exception Folder.
        
        cmd3 = f'{n3cmd1} && {n3cmd2} && {n3cmd3} || {n3cmd4}'
        cmd4 = f'echo "Analysis for {filename} Started at {datetime.datetime.now()}" >> {DMCalc_logsPath}'
        cmd5 = f'echo "Analysis for {filename} Finished at {datetime.datetime.now()}" >> {DMCalc_logsPath}'
        
        try:
            run(cmd4, shell=True)
            run(cmd3, shell=True)
            run(cmd5, shell=True)
            print(f'{filename} Analysis succesful!')
            time.sleep(1)
            Time_residual_plot(OutputPath,Pulsar)
            DM_Time_Series_Plot(OutputPath,Pulsar)
        except:
            print(f'{filename} Analysis failed')
    
        
        
while True:
    varst = monitor()

    if varst:
        for i in varst:
            copy(i)
        Missedfiles = scan()
        varst = varst + Missedfiles
        print('Files copied :', len(varst))
            
        threading.Thread(target=analysis, args=(varst,)).start()
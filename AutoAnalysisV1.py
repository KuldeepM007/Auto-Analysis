# File system monitoring

from subprocess import run
import datetime
import threading
import time
import matplotlib.pyplot as plt
import numpy as np


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


def copy(filename):
    Cycle,Pulsar,Band,Name = filename.split("/")[-4:]
    try:
        cmd2 = f'mkdir -p {LandDir}/{Cycle}/{Pulsar}/{Band}/; cp -d {filename} {LandDir}/{Cycle}/{Pulsar}/{Band}/'
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
        
        cmd3 = f' cd {OutputPath} && singularity exec {SingImgPath} {DMcalcPath} -E {ParPath}/{Pulsar}.par -M {ModelPath}/{Pulsar}{MBand} -nch 16 {TargetFilePath} >> {DMCalc_logsPath} && rm {TargetFilePath}'
        cmd4 = f'echo "Analysis for {filename} Started at {datetime.datetime.now()}" >> {DMCalc_logsPath}'
        cmd5 = f'echo "Analysis for {filename} Finished at {datetime.datetime.now()}" >> {DMCalc_logsPath}'
        
        try:
            run(cmd4, shell=True)
            run(cmd3, shell=True)
            run(cmd5, shell=True)
            print(f'{filename} Analysis succesful!')
            time.sleep(1)
        except:
            print(f'{filename} Analysis failed')
    
        Time_residual_plot(OutputPath,Pulsar)
        DM_Time_Series_Plot(OutputPath,Pulsar)
        
while True:

    varst = monitor()

    if varst:
        for i in varst:
            print(i, datetime.datetime.now())
            copy(i)
            
        threading.Thread(target=analysis, args=(varst,)).start()
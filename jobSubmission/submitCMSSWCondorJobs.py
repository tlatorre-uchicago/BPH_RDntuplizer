#!/usr/bin/env python
import os, sys, subprocess, re
import argparse
import commands
import time
from glob import glob

#____________________________________________________________________________________________________________
### processing the external os commands
def processCmd(cmd, quite = 0):
    status, output = commands.getstatusoutput(cmd)
    if (status !=0 and not quite):
        print 'Error in processing command:\n   ['+cmd+']'
        print 'Output:\n   ['+output+'] \n'
    return output

def createBatchName(a):
    try:
        aux = os.path.basename(a.input_file[0]).replace('inputFiles_', '').replace('.txt', '').replace('.root', '')
        n = aux
    except:
        n = a.maxtime
    return n

#_____________________________________________________________________________________________________________
#example line: python jobSubmission/submitCMSSWCondorJobs.py -i production/inputFiles_$process.txt -o $outLoc/$process/ntuples_B2DstMu/out_CAND.root -c $config --maxtime 120m -N 8 -f
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument ('-N', '--nFilePerJob', type=int, help='Number of files per job', default=10)
    parser.add_argument ('--nMaxJobs', type=int, help='Maximum number of jobs that will be submitted', default=None)
    parser.add_argument ('-i', '--input_file', type=str, default='', help='Input file template for glob or list in a txt format', nargs='+')
    parser.add_argument ('-o', '--output_file', type=str, default='', help='Output root file template')
    parser.add_argument ('-f', '--force_production', action='store_true', default=False, help='Proceed even if output file is already existing')
    parser.add_argument ('-c', '--config', type=str, help='Config file for cmsRUn')
    parser.add_argument ('--maxtime', help='Max wall run time [s=seconds, m=minutes, h=hours, d=days]', default='8h')
    parser.add_argument ('--memory', help='min virtual memory in MB', default='2000')
    parser.add_argument ('--disk', help='min disk space in MB', default='4000')
    parser.add_argument ('--cpu', help='cpu threads', default='1')
    parser.add_argument ('--name', type=str, default='ntuples', help='Job batch name')

    args = parser.parse_args()

    if not sys.argv[0].startswith('jobSubmission'):
        print 'You have to run from the BPH_RDntuplizer directory'
        exit()

    if 'X509_USER_PROXY' not in os.environ:
	print "X509_USER_PROXY environment variable not set"
	exit(1)

    '''
    ######################## Prepare output ###################################
    '''
    if not args.output_file:
        print 'No output file provided'
        exit()
    if not args.output_file.endswith('.root'):
        print 'Outputfile must end with .root'
        exit()
    outdir = os.path.dirname(args.output_file)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    else:
        if os.listdir(outdir):
            if args.force_production:
                os.system('rm -rf '+outdir+'/*')
            else:
                print 'Output directory not empty'
                print 'Empty the given directory, change directory or run with -f'
                exit()

    os.makedirs(outdir+'/out/')
    os.makedirs(outdir+'/cfg/')


    '''
    ################### Prepare input file and division #######################
    '''
    if not args.input_file:
        print 'No input file(s) provided'
        exit()

    flist = []
    if len(args.input_file) == 1 and args.input_file[0].endswith('.txt'):
        with open(args.input_file[0]) as f:
            for l in f.readlines():
                flist.append(l[:-1])
    elif len(args.input_file) == 1:
        flist = glob(args.input_file[0])
    elif isinstance(args.input_file, list):
        flist = args.input_file

    # print 'Trying to get a local copy'
    # for i in range(len(flist)):
    #     if flist[i].startswith('file:'):
    #         continue
    #     if os.path.isfile('/mnt/hadoop' + flist[i]):
    #         flist[i] = 'file:/mnt/hadoop' + flist[i]

    rem = len(flist)%args.nFilePerJob
    Njobs = len(flist)/args.nFilePerJob
    if rem: Njobs += 1

    print 'Input file provided:', len(flist)
    print 'Will be divided into', Njobs, 'jobs'
    for i in range(Njobs):
        i_start = i*args.nFilePerJob
        i_end = min((i+1)*args.nFilePerJob, len(flist))
        aux = '\n'.join(flist[i_start:i_end])
        with open(outdir + '/cfg/file_list_{}.txt'.format(i), 'w') as f:
            f.write(aux+'\n')

    if args.nMaxJobs:
        print 'Max number of jobs set to ', args.nMaxJobs
        if args.nMaxJobs < Njobs:
            Njobs = args.nMaxJobs
            print 'Only', Njobs, 'will be submitted'
    if Njobs == 0: exit()

    '''
    ###################### Check CMSSW and config ############################
    '''

    if not args.config:
        print 'No config file provided'
        exit()
    if not os.path.exists(args.config):
        print 'Config non existing'
        exit()

    ntuplizer_loc = os.environ['PWD']


    ''' ###################### Creating the sub #############################'''
    time_scale = {'s':1, 'm':60, 'h':60*60, 'd':60*60*24}
    maxRunTime = int(args.maxtime[:-1]) * time_scale[args.maxtime[-1]]

    os.system('chmod +x jobSubmission/CMSSWCondorJob.sh')
    print 'Creating submission scripts'

    with open('jobs.sub', 'w') as fsub:
        fsub.write('executable    = {}/jobSubmission/CMSSWCondorJob.sh\n'.format(os.environ['PWD']))

        exec_args = os.environ['PWD']+' '+args.config
        exec_args += ' ' + outdir + '/cfg/file_list_$(ProcId).txt'
        exec_args += ' ' + args.output_file.replace('.root', '_$(ProcId).root')
        fsub.write('arguments      = ' + exec_args)
        fsub.write('\n')
        fsub.write('output         = {}/out/job_$(ProcId)_$(ClusterId).out'.format(outdir))
        fsub.write('\n')
        fsub.write('error          = {}/out/job_$(ProcId)_$(ClusterId).err'.format(outdir))
        fsub.write('\n')
        fsub.write('log            = {}/out/job_$(ProcId)_$(ClusterId).log'.format(outdir))
        fsub.write('\n')
        fsub.write('+MaxRuntime    = '+str(maxRunTime))
        fsub.write('\n')
        if maxRunTime >= 7800:
            fsub.write('+JobQueue="Normal"')
        else:
            fsub.write('+JobQueue="Short"')
        fsub.write('\n')
        if os.uname()[1] == 'login-2.hep.caltech.edu':
            fsub.write('+RunAsOwner = True')
            fsub.write('\n')
            fsub.write('+InteractiveUser = True')
            fsub.write('\n')
            # Check for the right one using: ll /cvmfs/singularity.opensciencegrid.org/cmssw/
            fsub.write('+SingularityImage = "/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel7"')
            fsub.write('\n')
            fsub.write('+SingularityBindCVMFS = True')
            fsub.write('\n')
            fsub.write('run_as_owner = True')
            fsub.write('\n')
            fsub.write('RequestDisk = ' + args.disk)
            fsub.write('\n')
            # fsub.write('RequestMemory = ' + args.memory) #Static allocation
            fsub.write('request_memory = ifthenelse(MemoryUsage =!= undefined, MAX({{MemoryUsage + 1024, {0}}}), {0})'.format(args.memory)) # Dynamic allocation
            fsub.write('\n')
            fsub.write('RequestCpus = ' + args.cpu)
            fsub.write('\n')
        fsub.write('+JobBatchName  = '+args.name)
        fsub.write('\n')
        fsub.write('x509userproxy  = $ENV(X509_USER_PROXY)')
        fsub.write('\n')
        fsub.write('on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)')
        fsub.write('\n')
        fsub.write('on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)')   # Send the job to Held state on failure.
        fsub.write('\n')
        fsub.write('periodic_release =  (NumJobStarts < 3) && ((CurrentTime - EnteredCurrentStatus) > (60*10))')   # Periodically retry the jobs for 3 times with an interval 20 mins.
        fsub.write('\n')
        fsub.write('max_retries    = 3')
        fsub.write('\n')
        fsub.write('requirements   = Machine =!= LastRemoteHost && regexp("blade-.*", TARGET.Machine)')
        fsub.write('\n')
        fsub.write('universe = vanilla')
        fsub.write('\n')
        fsub.write('queue '+str(Njobs))
        fsub.write('\n')

    print 'Submitting jobs'
    cmd = 'condor_submit jobs.sub'
    cmd += ' -batch-name '+args.name+'_' + createBatchName(args)
    output = processCmd(cmd)
    print 'Job submitted'
    os.system('mv jobs.sub '+outdir+'/cfg/jobs.sub')

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
#example line: python jobSubmission/submitCMSSWCondorJobs.py -i /eos/user/o/ocerri/BPhysics/data/cmsMC_private/BPH_Tag-B0_MuNuDmst-pD0bar-kp_13TeV-pythia8_SoftQCD_PTFilter5_0p0-evtgen_HQET2_central_PU35_10-2-3_v0/jobs_out/*MINIAODSIM*.root -o /afs/cern.ch/user/o/ocerri/cernbox/BPhysics/data/cmsMC_private/BPH_Tag-B0_MuNuDmst-pD0bar-kp_13TeV-pythia8_SoftQCD_PTFilter5_0p0-evtgen_HQET2_central_PU35_10-2-3_v0/MuDst_candidates/out.root -f -c config/cmssw_privateMC_Tag_MuDmst-pD0bar-kp.py --maxtime 8h -N 5 --name MC
if __name__ == "__main__":
    import sqlite3
    from os.path import join
    import uuid

    parser = argparse.ArgumentParser()

    parser.add_argument ('-N', '--nFilePerJob', type=int, help='Number of files per job', default=10)
    parser.add_argument ('-i', '--input_file', type=str, default='', help='Input file template for glob or list in a txt format', nargs='+')
    parser.add_argument ('-o', '--output_file', type=str, default='', help='Output root file template')
    parser.add_argument ('-f', '--force_production', action='store_true', default=False, help='Proceed even if output file is already existing')
    parser.add_argument ('-c', '--config', type=str, help='Config file for cmsRUn')
    parser.add_argument ('--maxtime', help='Max wall run time [s=seconds, m=minutes, h=hours, d=days]', default='8h')
    parser.add_argument ('--memory', help='min virtual memory in MB', default='2000')
    parser.add_argument ('--disk', type=int, help='min disk space in MB', default=4000)
    parser.add_argument ('--cpu', type=int, help='cpu threads', default=1)
    parser.add_argument ('--name', type=str, default='ntuples', help='Job batch name')
    parser.add_argument("--db", type=str, help="database file", default=None)

    args = parser.parse_args()

    if args.db is None:
        home = os.path.expanduser("~")
        args.db = join(home,'state.db')

    conn = sqlite3.connect(args.db)

    c = conn.cursor()

    # Create the database table if it doesn't exist
    c.execute("CREATE TABLE IF NOT EXISTS ntuplizer_jobs ("
        "id             INTEGER PRIMARY KEY, "
        "timestamp      DATETIME DEFAULT CURRENT_TIMESTAMP, "
        "submit_file    TEXT NOT NULL UNIQUE, "
        "log_file       TEXT NOT NULL UNIQUE, "
        "output_file    TEXT NOT NULL UNIQUE, "
        "input_files    TEXT NOT NULL, "
        "batch_name     TEXT NOT NULL, "
        "uuid           TEXT NOT NULL, "
        "state          TEXT NOT NULL, "
        "nretry         INTEGER,"
        "message        TEXT,"
        "priority       INTEGER DEFAULT 1)"
    )

    conn.commit()

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
                os.system('rm -rf ' + outdir + '/*')
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

    results = c.execute('SELECT id, input_files FROM ntuplizer_jobs ORDER BY timestamp ASC')

    for row in results.fetchall():
        id, input_files = row
        input_files = input_files.split(",")

        for filename in input_files:
            if filenamein flist:
                print("skipping %s because it's already done" % filename)
                flist.remove(filename)

    rem = len(flist) % args.nFilePerJob
    Njobs = len(flist)/args.nFilePerJob
    if rem:
        Njobs += 1

    print 'Input file provided:', len(flist)
    print 'Will be divided into', Njobs, 'jobs'

    if Njobs == 0:
        print("No jobs to run")
        exit()

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

    job_submission_dir_path = os.path.dirname(os.path.realpath(__file__))

    os.system('chmod +x %s/CMSSWCondorJob.sh' % job_submission_dir_path)
    print 'Creating submission scripts'

    for i in range(Njobs):
        i_start = i*args.nFilePerJob
        i_end = min((i+1)*args.nFilePerJob, len(flist))
        files = flist[i_start:i_end]
        with open(join(outdir,'cfg/file_list_%i.txt' % i), 'w') as f:
            f.write('\n'.join(files) + '\n')

        submit_file = os.path.realpath(join(outdir,'jobs_%i.sub' % i))
        with open(submit_file, 'w') as fsub:
            # generate a UUID to append to all the filenames so that if we run the same job
            # twice we don't overwrite the first job
            ID = uuid.uuid1()

            fsub.write('executable    = %s/CMSSWCondorJob.sh\n' % job_submission_dir_path)

            exec_args = job_submission_dir_path
            exec_args += ' %s' % os.path.realpath(args.config)
            exec_args += ' %s' % join(outdir,'cfg/file_list_%i.txt' % i)
            output_file = join(args.output_file.replace('.root', '_%i.root' % i))
            exec_args += ' %s' % output_file
            fsub.write('arguments      = %s\n' % exec_args)
            log_file = '%s/out/job_%i.out\n' % (outdir,i)
            fsub.write('output         = %s\n' % log_file)
            fsub.write('error          = %s/out/job_%i.err\n' % (outdir,i))
            fsub.write('log            = %s/out/job_%i.log\n' % (outdir,i))
            fsub.write('+MaxRuntime    = %i\n' % maxRunTime)
            if os.uname()[1] == 'login-1.hep.caltech.edu':
                fsub.write('+RunAsOwner = True\n')
                fsub.write('+InteractiveUser = True\n')
                # Check for the right one using: ll /cvmfs/singularity.opensciencegrid.org/cmssw/
                fsub.write('+SingularityImage = "/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel7-m20200724"\n')
                fsub.write('+SingularityBindCVMFS = True\n')
                fsub.write('run_as_owner = True\n')
                fsub.write('RequestDisk = %i\n' % args.disk)
                fsub.write('request_memory = ifthenelse(MemoryUsage =!= undefined, MAX({{MemoryUsage + 1024, {0}}}), {0})\n'.format(args.memory)) # Dynamic allocation
                fsub.write('RequestCpus = %i\n' % args.cpu)
            fsub.write('+JobBatchName  = %s\n' % args.name)
            fsub.write('+UUID  = "%s"\n' % ID.hex)
            fsub.write('x509userproxy  = $ENV(X509_USER_PROXY)\n')
            fsub.write('on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)\n')
            # Send the job to Held state on failure.
            fsub.write('on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)\n')
            # Periodically retry the jobs for 3 times with an interval 20 mins.
            fsub.write('periodic_release =  (NumJobStarts < 5) && ((CurrentTime - EnteredCurrentStatus) > (60*10))\n')
            fsub.write('max_retries    = 5\n')
            fsub.write('requirements   = Machine =!= LastRemoteHost && regexp("blade-.*", TARGET.Machine)\n')
            fsub.write('universe = vanilla\n')
            fsub.write('queue 1\n')

        c.execute("INSERT INTO ntuplizer_jobs ("
            "submit_file    , "
            "log_file       , "
            "output_file    , "
            "input_files    , "
            "batch_name     , "
            "uuid           , "
            "state          , "
            "nretry         ) "
            "VALUES (?, ?, ?, ?, ?, ?, 'NEW', NULL)",
            (submit_file, log_file, output_file, ','.join(files), '%s_%s' % (args.name,createBatchName(args)), ID.hex))

    conn.commit()

    #print 'Submitting jobs'
    #cmd = 'condor_submit jobs.sub'
    #cmd += ' -batch-name '+args.name+'_' + createBatchName(args)
    #output = processCmd(cmd)
    #print 'Job submitted'
    #os.system('mv jobs.sub '+outdir+'/cfg/jobs.sub')

#!/usr/bin/env python

# Import stuff.

import sys, os, project_utilities

# Import ROOT (hide command line arguments).

myargv = sys.argv
sys.argv = myargv[0:1]
sys.argv.append('-n')
# Prevent root from printing garbage on initialization.
if os.environ.has_key('TERM'):
    del os.environ['TERM']
import ROOT
ROOT.gErrorIgnoreLevel = ROOT.kError
sys.argv = myargv

# This function opens an artroot file and extracts the list of runs and subruns
# from the SubRuns TTree.
# A list of (run, subrun) pairs is returned as a list of 2-tuples.

def get_subruns(inputfile):

    # Initialize return value to empty list.

    result = []

    # Check whether this file exists.
    if not os.path.exists(inputfile):
        return result
            
    # Root checks.

    file = project_utilities.SafeTFile(inputfile)
    if file and file.IsOpen() and not file.IsZombie():

        # Root file opened successfully.
        # Get runs and subruns fro SubRuns tree.

        subrun_tree = file.Get('SubRuns')
        if subrun_tree and subrun_tree.InheritsFrom('TTree'):
            nsubruns = subrun_tree.GetEntriesFast()
            tfr = ROOT.TTreeFormula('subruns',
                                    'SubRunAuxiliary.id_.run_.run_',
                                    subrun_tree)
            tfs = ROOT.TTreeFormula('subruns',
                                    'SubRunAuxiliary.id_.subRun_',
                                    subrun_tree)
            for entry in range(nsubruns):
                subrun_tree.GetEntry(entry)
                run = tfr.EvalInstance64()
                subrun = tfs.EvalInstance64()
                run_subrun = (run, subrun)
                result.append(run_subrun)

    else:

        # Root file could not be opened.

        result = []

    # Sort in order of increasing run, then increasing subrun.

    result.sort()

    # Done.

    return result

if __name__ == "__main__":
    run_subruns = get_subruns(str(sys.argv[1]))
    for run_subrun in run_subruns:
        print run_subrun[0], run_subrun[1]
    sys.exit(0)	

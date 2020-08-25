sleep 5
next_stage_input=`ls -t1 *.root | egrep -v 'celltree|hist|larlite|larcv|Supplemental|TGraphs' | head -n1`
echo "ln -sf $next_stage_input input${stage}.root"
ln -sf $next_stage_input input${stage}.root
next_stage_input=input${stage}.root


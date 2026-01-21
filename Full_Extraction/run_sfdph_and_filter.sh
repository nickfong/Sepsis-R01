#!/bin/bash
# Submit SFDPH extraction and queue filtering to run after it completes

cd /wynton/protected/home/pirracchio/nick/Sepsis-R01/Full_Extraction

# Submit SFDPH extraction
EXTRACT_JOB=$(qsub run_sfdph_extraction.sge | awk '{print $3}')
echo "Submitted SFDPH extraction job: $EXTRACT_JOB"

# Submit filter job with hold on extraction
FILTER_JOB=$(qsub -hold_jid "$EXTRACT_JOB" run_filter.sge | awk '{print $3}')
echo "Submitted filter job: $FILTER_JOB (holding on $EXTRACT_JOB)"

echo ""
echo "Monitor with: qstat"

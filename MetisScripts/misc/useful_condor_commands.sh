
alias cond='condor_q $USER'
alias conf='condor_q $USER -nobatch'
alias conda='condor_q -all'
alias cstat='condor_q $USER | col 6 | drop 4 | head -n -2 | sort | uniq -c'
alias cstata='condor_q | col 6 | drop 4 | head -n -2 | sort | uniq -c'

function drop {
# drops the first n lines
if [ $# -lt 1 ]; then
    echo "usage: drop <drop #>"
    return 1
fi
n=$1
tail --lines=+$((n+1))
}

function col {
if [ $# -lt 1 ]; then
    echo "usage: col <col #>"
    return 1
fi
num=$1

if [[ $num -lt 0 ]]; then 
    awk "{print \$(NF+$((num+1)))}"
else
    awk -v x=$num '{print $x}'
fi
}

function cw {
    condor_q $1 -const 'JobStatus==2' -l | grep "^RemoteHost" | rev | cut -d '@' -f1 | rev | cut -d '"' -f1 | cut -d '-' -f1
}
function csites {
    condor_q $1 -const 'JobStatus==2' -af MATCH_EXP_JOB_Site | cut -d'@' -f2 | cut -d'"' -f2 | sort | uniq -c
}
function csitesi {
    condor_q -const 'JobStatus==1' -af DESIRED_Sites | sort | uniq -c
}
function cwh {
    cw | cut -d'-' -f1 | sort | uniq -c
}

function clog {
    num=20
    # if number is less than 10k, then it can't be a condor_id, so
    # use it as the number of entries to show, otherwise use it
    # as condor_id
    if [ $# -gt 0 ]; then 
        num=$(echo $1 | sed 's/\..*//')
    fi
    if  [[ $# -gt 0 && "$num" -gt 10000 ]]; then
        jobid=$1
        info=$(condor_history  $jobid -limit 1 -af Iwd Out Err)
        iwd=$(echo $info | awk '{print $1}')
        out=$(echo $info | awk '{print $2}')
        err=$(echo $info | awk '{print $3}')
        [[ "$out" == "/"* ]] || out=${iwd}/${out}
        [[ "$err" == "/"* ]] || err=${iwd}/${err}
        echo $out
        echo $err
        vim -O $out $err  -c "normal G"
    else
        # condor_history $USER -limit 100
        condor_history $USER -limit $num
    fi
}

function condh {
    condor_q -l $1 | grep -i "hold"
}


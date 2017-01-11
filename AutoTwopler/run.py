#!/usr/bin/env python

import Samples
import utils as u
import time
import pprint
import datetime
import json
import sys
import traceback
import os
import logging

def main(instructions=None, params=None):
    if not instructions:
        return

    if not params:
        params = __import__('params')

    data_json = "data.json"
    actions_fname = os.path.abspath(__file__).rsplit("/",1)[0]+"/actions.txt"

    u.copy_jecs()
    logger_name = u.setup_logger()
    logger = logging.getLogger(logger_name)


    time_stats = []
    if os.path.isfile(data_json):
        with open(data_json, "r") as fhin:
            data = json.load(fhin)
            if "time_stats" in data: time_stats = data["time_stats"]

    all_samples = []
    for i in range(5000):

        if u.proxy_hours_left() < 60 and not params.FORSAKE_HEAVENLY_PROXY: u.proxy_renew()

        data = { "samples": [], "last_updated": None, "time_stats": time_stats }

        # read instructions file. if new sample found, add it to list
        # for existing samples, try to update params (xsec, kfact, etc.)
        for samp in u.read_samples(instructions):
            samp["params"] = params
            if samp not in all_samples:
                s = Samples.Sample(**samp) 
                all_samples.append(s)
            else:
                all_samples[all_samples.index(samp)].update_params(samp)


        n_done = 0
        n_samples = len(all_samples)
        for isample, s in enumerate(all_samples):

            try:
                stat = s.get_status()
                typ = s.get_type()

                # grab actions from a text file and act on them, consuming (removing) them if successful
                for dataset_name, action in u.get_actions(actions_fname=actions_fname,dataset_name=s["dataset"]):
                    if s.handle_action(action):
                        u.consume_actions(dataset_name=s["dataset"],action=action, actions_fname=actions_fname)

                if not s.pass_tsa_prechecks(): continue


                if typ == "CMS3":

                    if stat == "new":
                        s.crab_submit()
                    elif stat == "crab":
                        s.crab_parse_status()
                        if s.is_crab_done():
                            s.make_miniaod_map()
                            s.make_merging_chunks()
                            s.submit_merge_jobs()
                    elif stat == "postprocessing":
                        if s.is_merging_done():
                            if s.check_output():
                                s.make_metadata()
                                s.copy_files()
                        else:
                            s.submit_merge_jobs()
                    elif stat == "done":
                        s.do_done_stuff()
                        n_done += 1

                elif typ == "BABY":
                    
                    if stat == "new":
                        s.set_baby_inputs()
                        s.submit_baby_jobs()

                    elif stat == "condor":
                        if s.is_babymaking_done():
                            s.set_status("done")
                        else:
                            s.sweep_babies()
                            s.submit_baby_jobs()

                    elif stat == "done":
                        s.do_done_stuff()
                        if params.open_datasets:
                            s.check_new_merged_for_babies()
                        else:
                            n_done += 1


                s.save()
                data["samples"].append( s.get_slimmed_dict() )

            except Exception, err:
                logger.info( "send an (angry?) email to Nick with the Traceback below!!")
                logger.info( traceback.format_exc() )

        breakdown_crab = u.sum_dicts([samp["crab"]["breakdown"] for samp in data["samples"] if "crab" in samp and "breakdown" in samp["crab"]])
        # breakdown_baby = u.sum_dicts([{"baby_"+key:samp["baby"].get(key,0) for key in ["running", "sweepRooted"]} for samp in data["samples"] if samp["type"] == "BABY"])
        breakdown_baby = u.sum_dicts([{"running_babies":samp["baby"]["running"], "sweepRooted_babies":samp["baby"]["sweepRooted"]} for samp in data["samples"] if samp["type"] == "BABY"])
        tot_breakdown = u.sum_dicts([breakdown_crab, breakdown_baby])
        data["last_updated"] = u.get_timestamp()
        data["time_stats"].append( (u.get_timestamp(), tot_breakdown) )
        data["log"] = u.get_last_n_lines(fname=params.log_file, N=100)
        with open(data_json, "w") as fhout:
            data["samples"] = sorted(data["samples"], key=lambda x: x.get("status","done")=="done")
            json.dump(data, fhout, sort_keys = True, indent = 4)
        u.copy_json()

        if params.exit_when_done and (n_done == n_samples):
            print ">>> All %i samples are done. Exiting." % n_samples
            sys.exit()

        sleep_time = 5 if i < 2 else 600
        logger.debug("sleeping for %i seconds..." % sleep_time)
        u.smart_sleep(sleep_time, files_to_watch=[actions_fname, instructions])

if __name__ == "__main__":

    instructions = "instructions.txt"
    if len(sys.argv) > 1:
        instructions = sys.argv[1]
    else:
        print ">>> Note usage is: %s INSTRUCTIONS_FILE" % __file__
        print ">>> But going ahead and using instructions.txt for convenience"

    if not os.path.isfile(instructions):
        print ">>> %s does not exist" % instructions
        sys.exit()

    main(instructions=instructions)


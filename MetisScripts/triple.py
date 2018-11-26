import time
import traceback

from metis.StatsParser import StatsParser
from metis.Utils import send_email, interruptible_sleep

import data2016_94x_v2
import data2017_94x_v2
import mc2017_94x_v2
import mc2016_94x_v3
import data2018_102x
import prompt10x_data
import moriondremc # 2016 80X CMS4, CMS4_V00-00-02_2017Sep27 tag
import moriondremc_private # same as above, private samples

if __name__ == "__main__":

    for i in range(10000):
        total_summary = {}
        tasks = []
        # tasks.extend(data2016_94x_v2.get_tasks())
        # tasks.extend(mc2016_94x_v3.get_tasks())
        # tasks.extend(data2017_94x_v2.get_tasks())
        tasks.extend(mc2017_94x_v2.get_tasks())
        tasks.extend(data2018_102x.get_tasks())
        tasks.extend(prompt10x_data.get_tasks())
        tasks.extend(moriondremc.get_tasks())
        tasks.extend(moriondremc_private.get_tasks())
        for task in tasks:
            dsname = task.get_sample().get_datasetname()
            try:
                if not task.complete(): task.process()
            except:
                traceback_string = traceback.format_exc()
                print "Runtime error:\n{0}".format(traceback_string)
                send_email(subject="metis error", body=traceback_string)
            total_summary[dsname] = task.get_task_summary()
        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/", make_plots=False).do(show_progress_bar=True)

        interruptible_sleep(3*3600, reload_modules=[
            data2016_94x_v2,
            data2017_94x_v2,
            mc2017_94x_v2,
            mc2016_94x_v3,
            data2018_102x,
            prompt10x_data,
            moriondremc,
            moriondremc_private,
            ])

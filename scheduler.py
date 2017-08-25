#!/usr/bin/pyton3
# -*- coding: utf-8 -*-

import run
from apscheduler.schedulers.blocking import BlockingScheduler

def parse_sync():
    run.main(action='sync')

def yield_report():
    run.main()

if __name__ == '__main__':
    sched = BlockingScheduler()
    sched.add_job(parse_sync, 'interval', minutes=1)
    sched.add_job(yield_report, 'cron', hour=20, minute=30)
    sched.start()
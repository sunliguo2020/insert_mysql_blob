# -*- coding: utf-8 -*-
"""
@author: sunliguo
@contact: QQ376440229
@Created on: 2022/6/12 13:54
"""
import sys
import time

import psutil

#get pid from args
if len(sys.argv) <2:
    print("missing pid arg")
    sys.exit(1)

pid = int(sys.argv[1])
p = psutil.Process(pid)

interval = 3 #polling seconds
with open("proc"+p.name()+str(pid)+'.csv','a') as fp:
    fp.write('time,cpu%,mem%\n')
    while True:
        current_time = time.strftime('%Y%m%d-%H%M%S',time.localtime(time.time()))
        cpu_percent = p.cpu_percent()
        mem_percent = p.memory_percent()
        fp.write(f"{current_time},{str(cpu_percent)},{str(mem_percent)}\n")
        time.sleep(interval)
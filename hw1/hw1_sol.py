# ### * BEGIN STUDENT CODE *

# In[4]:

import apachetime
import time

def apache_ts_to_unixtime(ts):
    """
    @param ts - a Apache timestamp string, e.g. '[02/Jan/2003:02:06:41 -0700]'
    @returns int - a Unix timestamp in seconds
    """
    dt = apachetime.apachetime(ts)
    unixtime = time.mktime(dt.timetuple())
    return int(unixtime)


# In[5]:

import csv

def process_logs(dataset_iter):
    """
    Processes the input stream, and outputs the CSV files described in the README.    
    This is the main entry point for your assignment.
    
    @param dataset_iter - an iterator of Apache log lines.
    """
    ### hits.csv ###
    with open("hits.csv", "w+") as hits_file:
        fieldnames = ['ip', 'timestamp']
        writer = csv.DictWriter(hits_file, lineterminator="\n", fieldnames=fieldnames)
        
        writer.writeheader()
        for line in dataset_iter:
            parsed = line.split(" ")
            ip = parsed[0]
            time = parsed[3] + parsed[4]
            t = apache_ts_to_unixtime(time)
            writer.writerow({'ip': ip, 'timestamp': t})
        
        print "Done."
    
    ### sessions.csv ###
    get_ipython().system(u'cat hits.csv | sort -n -k 1 > temp.csv')

    with open("sessions.csv", "w+") as sessions_file:
        fieldnames = ['ip', 'session_length', 'num_hits']
        writer = csv.DictWriter(sessions_file, lineterminator="\n", fieldnames=fieldnames)
        
        writer.writeheader()
        
        with open("temp.csv", "rb") as tmp:
            reader = csv.reader(tmp)
            
            ip = ""
            count = 0
            init_time = 0
            next_time = 0
            
            for row in reader:
                current_ip = row[0]
                if current_ip != 'ip':
                    t = int(row[1])
                    time = next_time - init_time
                    if ip == "":
                        ip = current_ip
                        init_time = t
                        next_time = t
                        count += 1
                    elif current_ip == ip:
                        if t <= next_time + 1800:
                            next_time = t
                            count += 1
                        else:
                            writer.writerow({'ip': ip, 'session_length': time, 'num_hits': count})
                            init_time = t
                            next_time = t
                            count = 1
                    else:
                        writer.writerow({'ip': ip, 'session_length': time, 'num_hits': count})
                        ip = current_ip
                        init_time = t
                        next_time = t
                        count = 1
            time = next_time - init_time
            writer.writerow({'ip': ip, 'session_length': time, 'num_hits': count})
        
        get_ipython().system(u'rm temp.csv')
        
        print "Done."
        

    ### session_length_plot.csv ###
    get_ipython().system(u'cat sessions.csv | sort -t "," -n -k 2 > temp2.csv')
    
    with open("session_length_plot.csv", "w+") as plot_file:
        fieldnames = ['left', 'right', 'count']
        writer = csv.DictWriter(plot_file, lineterminator="\n", fieldnames=fieldnames)
        
        writer.writeheader()
        
        with open("temp2.csv", "rb") as tmp:
            reader = csv.reader(tmp)
            
            left = 0
            right = 2
            count = 0
            
            for row in reader:
                current_ip = row[0]
                
                if current_ip != 'ip':
                    session = int(row[1])
                    hits = int(row[2])
                    
                    if session >= left and session < right:
                        count += 1
                    else:
                        if count != 0:
                            writer.writerow({'left': left, 'right': right, 'count': count})
                        while not (session >= left and session < right):
                            left = right
                            right *= 2
                        count = 1
            writer.writerow({'left': left, 'right': right, 'count': count})
    
        get_ipython().system(u'rm temp2.csv')
    
        print "Done."


# ### * END STUDENT CODE *
import os
DATA_DIR = os.environ['MASTERDIR'] + '/sp16/hw1/'

import zipfile

def process_logs_large():
    """
    Runs the process_logs function on the full dataset.  The code below 
    performs a streaming unzip of the compressed dataset which is (158MB). 
    This saves the 1.6GB of disk space needed to unzip this file onto disk.
    """
    with zipfile.ZipFile(DATA_DIR + "web_log_large.zip") as z:
        fname = z.filelist[0].filename
        f = z.open(fname)
        process_logs(f)
        f.close()
process_logs_large()

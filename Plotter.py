#Plotting Libraries
import os
import matplotlib.pyplot as plt
import numpy as np
import math
import scipy.stats
from tqdm import *

#Other imports
import LogParser as lp


#Takes list of log file paths, outputs list of log file dictionaries -> {"key":[list of values]}
#Uses tqdm package to display progress bar
def tqdm_parse_log_files(fPile):
    logObjPile = {}
    counter = 0

    for fpath in tqdm(fPile):
        logObjPile[counter] = lp.log_parser(fpath)
        counter += 1

    return logObjPile

#Takes list of log file paths, outputs list of log file dictionaries -> {"key":[list of values]}
def updt_log_dict(logObjPile, fPile):
    counter = len(logObjPile)

    for fpath in fPile:
        logObjPile[counter] = lp.log_parser(fpath)
        counter += 1

    return logObjPile

#Log File Functions:
#Imports os, retrieves .out files
def get_log_files(logdir, ftype):
    import os
    
    #.log in .../logs, .err and .out in .../logs/std_logs
    if ftype == ".log":
        endpath = "/logs"
    else:
        endpath = "/logs/std_logs"
    
    fPile = []
    dirLst = os.listdir(logdir)
    
    for d in dirLst:
        if d == "plots":
            continue
        if d == "summary.json":
            continue
        else:
            newdir = (os.path.abspath(os.path.join(logdir, d)) + endpath)
            newdirLst = os.listdir(newdir) 
            for f in newdirLst:
                if f.endswith(ftype):
                    fPile.append(os.path.abspath(os.path.join(newdir, f)))
                
    return fPile

#Retrieve perinent files from "condor_jobs" key in json output
#takes log dicionary, condor_jobs list, desired file type, and path to directry with log files
def get_json_files(logObjPile, condor_jobs, ftype, usrpath):
    fPile = []
    
    ftype = ("logfile_" + ftype.split(".")[1])

    for log in condor_jobs:
        try:
            fpath = usrpath + log[ftype].split("ProjectMetis/tasks")[1]
            fPile.append(fpath)
        except KeyError:
            pass

    return updt_log_dict(logObjPile, fPile)

#Plotting Functions
#Sets plot title and axis labels, shows graph
def set_graph_info(title, xlabel, ylabel):

    plt.title("")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    split_title = title.split('/')
    file_title = ''
    for wrd in split_title:
        if wrd != '':
            file_title += (wrd + '_')
    
    pltpath = ('plots/' + file_title + xlabel + '_vs_' + ylabel + '.png')

    if not os.path.exists("plots/"):
        os.mkdir("plots/")
    plt.savefig(pltpath)
    plt.close()
    
    return (pltpath)

#Takes data as list, returns mean as float
def get_mean(data):
    counter = 0
    nSum = 0
    for num in data:
        nSum += num
        counter += 1

    return (float(nSum)/float(counter))

def get_zeroed_times(logObjPile):
    lst = []
    for log in logObjPile:
        try:
            tstart = logObjPile[log]['epoch'][0]
            for pnt in logObjPile[log]['epoch']:
                lst.append(float(pnt) - float(tstart))
        except (KeyError, IndexError) as error:
            pass

    return lst

def get_data_1D(logObjPile, key):
    lst = []
    for log in logObjPile:
        try:
            for pnt in logObjPile[log][key]:
                lst.append(float(pnt))
        except (KeyError, IndexError) as error:
            pass

    return lst

def get_data_2D(logObjPile, xkey, ykey):
    xLst = []
    yLst = []
    if xkey == "epoch":
        xLst = get_zeroed_times(logObjPile)
    else:
        xLst = get_data_1D(logObjPile, xkey)
    if ykey == "epoch":
        yLst = get_zeroed_times(logObjPile)
    else:
        yLst = get_data_1D(logObjPile, ykey)

    return xLst, yLst
    
def plot_1DHist(logObjPile, title, xkey, bins):
    x = get_data_1D(logObjPile, xkey)

    plt.hist(x, bins)

    return set_graph_info(title, xkey, "") 

def plot_2DHist(logObjPile, title, xkey, ykey, bins, norm_toggle):
    #Get data
    x, y = get_data_2D(logObjPile, xkey, ykey)
    
    if norm_toggle == 1:
        max_x = float(max(x))
        counter = 0
        for num in x:
            x[counter] = (num/max_x)
            counter += 1


    #Import Colors
    from matplotlib.colors import LogNorm

    #Create Heatmap
    plt.hist2d(x, y, bins = bins, norm = LogNorm())

    #Plot Heatmap
    plt.colorbar()

    return set_graph_info(title, xkey, ykey)

def plot_Profile(logObjPile, title, xkey, ykey, bins, norm_toggle):
    #Get data
    x, y = get_data_2D(logObjPile, xkey, ykey)
    x = np.array(x)
    y = np.array(y)
    
    if norm_toggle == 1:
        max_x = float(max(x))
        counter = 0
        for num in x:
            x[counter] = (num/max_x)
            counter += 1

    #Build graph
    means_result = scipy.stats.binned_statistic(x, [y, y**2], bins = bins, statistic = "mean")
    means, means2 = means_result.statistic
    standard_deviation = np.sqrt(means2 - means**2)
    bin_edges = means_result.bin_edges
    bin_centers = (bin_edges[:-1] + bin_edges[1:])/2.0
    
    #Plot graph
    plt.errorbar(x = bin_centers, y = means, yerr = standard_deviation, linestyle = "none", marker = ".")

    return set_graph_info(title, xkey, ("Average " + ykey))

#Debugging operations
if __name__ == "__main__":
    fPile = get_log_files("/home/jguiang/ProjectMetis/log_files/tasks", ".out")
    logObjPile = tqdm_parse_log_files(fPile)

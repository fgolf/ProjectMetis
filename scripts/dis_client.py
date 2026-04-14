#!/usr/bin/env python3
"""
DIS Client — queries the DIS (Dataset Information Service) server.

Examples:
    dis_client.py -t snt "*,cms3tag=CMS3_V08-00-01 | grep dataset_name,nevents_in,nevents_out"
    dis_client.py /GJets_HT-600ToInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15DR74-Asympt25ns_MCRUN2_74_V9-v1/MINIAODSIM
    dis_client.py -t files /Dataset/Path/TIER
    dis_client.py -t files -d /Dataset/Path/TIER

Or import and query programmatically:
    import dis_client
    data = dis_client.query(q="/Dataset/Path/TIER", typ="basic")
"""

import json
import sys
import argparse
import time
import glob
import os
from urllib.request import urlopen
from urllib.parse import urlencode

# BASEURL is configurable via DIS_URL environment variable.
# Falls back to localhost:50010 (the new dis_server.py) if not set.
BASEURL = os.environ.get(
    "DIS_URL",
    "http://localhost:50010/dis/serve"
)


def query(q, typ="basic", detail=False, timeout=999):
    """
    Query the DIS server.

    Args:
        q: Query string (dataset path, SNT filter, etc.)
        typ: Query type — basic, files, snt, update_snt, config, parents, runs, sites, mcm, chain
        detail: If True, return detailed (non-short) results
        timeout: HTTP timeout in seconds

    Returns:
        Dict with "status" and "payload" keys.
    """
    query_dict = {"query": q, "type": typ, "short": "" if detail else "short"}
    url = "%s?%s" % (BASEURL, urlencode(query_dict))

    data = {}
    try:
        content = urlopen(url, timeout=timeout).read()
        data = json.loads(content)
    except Exception as e:
        print("Failed to perform URL fetching and decoding: %s" % str(e))

    return data


def listofdicts_to_table(lod):  # pragma: no cover
    """Format a list of dicts as an ASCII table."""
    colnames = list(set(sum([list(thing.keys()) for thing in lod], [])))

    d_colsize = {}
    for thing in lod:
        for colname in colnames:
            val = str(thing.get(colname, ""))
            if colname not in d_colsize:
                d_colsize[colname] = len(colname) + 1
            d_colsize[colname] = max(len(val) + 1, d_colsize[colname])

    colnames = sorted(colnames, key=d_colsize.get, reverse=True)

    try:
        from pytable import Table
        if not sys.stdout.isatty():
            raise Exception

        tab = Table()
        tab.set_column_names(colnames)
        for row in lod:
            tab.add_row([row.get(colname) for colname in colnames])
        tab.sort(column=colnames[0], descending=False)
        return "".join(tab.get_table_string())
    except Exception:
        buff = ""
        header = ""
        for icol, colname in enumerate(colnames):
            header += ("%%%s%is" % ("-" if icol == 0 else "", d_colsize[colname])) % colname
        buff += header + "\n"
        for thing in lod:
            line = ""
            for icol, colname in enumerate(colnames):
                tmp = "%%%s%is" % ("-" if icol == 0 else "", d_colsize[colname])
                tmp = tmp % str(thing.get(colname, ""))
                line += tmp
            buff += line + "\n"
        return buff


def get_output_string(q, typ="basic", detail=False, show_json=False, pretty_table=False, one=False):  # pragma: no cover
    """Format a DIS query result for terminal output."""
    buff = ""
    data = query(q, typ, detail)

    if not data:
        return "URL fetch/decode failure"

    if data.get("status") != "success":
        reason = "unknown"
        if isinstance(data.get("payload"), dict):
            reason = data["payload"].get("failure_reason", "unknown")
        return "DIS failure: %s" % reason

    data = data["payload"]

    if show_json:
        return json.dumps(data, indent=4)

    if isinstance(data, dict):
        if "files" in data:
            data = data["files"]

    if isinstance(data, list):
        if pretty_table:
            buff += listofdicts_to_table(data)
        else:
            for elem in data:
                if isinstance(elem, dict):
                    for key in elem:
                        buff += "%s:%s\n" % (key, elem[key])
                else:
                    buff += str(elem)
                buff += "\n"

    elif isinstance(data, dict):
        for key in data:
            buff += "%s: %s\n\n" % (key, data[key])

    if typ == "snt" and isinstance(data, list) and len(data) and one:
        location = data[0].get("location", "")
        if location:
            found = glob.glob("{}/*.root".format(location))
            if found:
                return found[0]

    buff = buff.rstrip()
    return buff


def test():  # pragma: no cover
    """Run a series of test queries against the DIS server."""
    queries = [
        {"type": "snt", "query": "/DY*/*MiniAOD*/MINIAODSIM | grep nevents_out | sort", "short": "short"},
        {"type": "snt", "query": "/G* | grep cms3tag"},
        {"type": "basic", "query": "/*/Run2016*-17Jul2018-v1/MINIAOD", "short": "short"},
        {"type": "basic", "query": "/DoubleMuon/Run2016*-17Jul2018-v1/MINIAOD"},
        {"type": "files", "query": "/SinglePhoton/Run2016E-PromptReco-v2/MINIAOD", "short": "short"},
        {"type": "parents", "query": "/SMS-T1tttt_mGluino-1500_mLSP-100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM"},
        {"type": "runs", "query": "/SinglePhoton/Run2016E-PromptReco-v2/MINIAOD"},
        {"type": "sites", "query": "/ZeroBias/Run2016F-17Jul2018-v1/MINIAOD", "short": "short"},
    ]

    green = "\033[92m"
    red = "\033[91m"
    clear = "\033[0m"

    try:
        columns = int(os.get_terminal_size().columns) - 20
    except Exception:
        columns = 80

    print(">>> Testing queries against %s" % BASEURL)
    for q_params in queries:
        detail = q_params.get("short", "") != "short"
        to_print = "{0}: {1}{2}".format(q_params["type"], q_params["query"], " (detailed)" if detail else "")
        if len(to_print) > columns:
            to_print = to_print[:columns - 3] + "..."
        t0 = time.time()
        data = query(
            q=q_params["query"],
            typ=q_params["type"],
            detail=detail,
            timeout=30,
        )
        t1 = time.time()
        status = data.get("status", "error")
        startcolor = green if status == "success" else red
        print("[{0}{1}{2} ({3:.2f}s)] {4}".format(startcolor, status, clear, t1 - t0, to_print))
        if status != "success" and isinstance(data.get("payload"), dict):
            print(data["payload"].get("failure_reason", ""))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DIS Client — query the Dataset Information Service")
    parser.add_argument("query", help="query string (dataset path, SNT filter, etc.)")
    parser.add_argument("-t", "--type", help="type of query (basic, files, snt, config, parents, runs, sites, mcm, chain)", default="basic")
    parser.add_argument("-d", "--detail", help="show more detailed information", action="store_true")
    parser.add_argument("-j", "--json", help="show output as full json", action="store_true")
    parser.add_argument("-p", "--table", help="show output as pretty table", action="store_true")
    parser.add_argument("-e", "--test", help="perform query tests", action="store_true")
    parser.add_argument("-o", "--one", help="get one file from SNT sample", action="store_true")
    parser.add_argument("-u", "--url", help="override DIS server URL", default=None)
    args = parser.parse_args()

    if args.url:
        BASEURL = args.url

    if args.test:
        test()
    else:
        print(get_output_string(args.query, typ=args.type, detail=args.detail, show_json=args.json, pretty_table=args.table, one=args.one))
